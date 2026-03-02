# OFI/libfabric Transport — Consensus Code Review

**Branch reviewed:** `feat/ofi-hybrid`
**Reviewers:** Claude Opus 4.6 + Gemini 3.1 Pro (consensus via PAL clink)
**Date:** 2026-03-01
**Scope:** 9 files, ~1,444 lines added

---

## Methodology

Both reviewers independently analyzed the full `feat/ofi-hybrid` diff, then findings
were cross-validated.  This document records: areas of agreement, areas of divergence,
and recommended fix priority.

---

## CRITICAL — Both Reviewers Agree

### C1. Heap buffer overflow on RX — unchecked `msglen` from wire

**File:** `src/sp/transport/ofi/ofi.c` — `ofi_pipe_drain_cqs` (RX CQ processing)

```c
NNI_GET64(p->rx_buf, msglen);
nni_msg_alloc(&msg, (size_t)msglen);  // no bounds check, no error check
memcpy(nni_msg_body(msg), (uint8_t*)p->rx_buf + 8, (size_t)msglen);
```

`msglen` is attacker-controlled (read from the wire).  Two distinct problems:

1. No validation that `msglen <= OFI_BOUNCE_SZ - 8` → heap over-read via `memcpy`.
2. Return value of `nni_msg_alloc` is ignored → NULL deref on OOM or on a
   maliciously large length.

**Fix:** Validate `msglen <= OFI_BOUNCE_SZ - 8` before allocation.  Check the
return value of `nni_msg_alloc`; on failure drop the message and close the pipe.

---

### C2. Heap buffer overflow on TX — no size check in `ofi_pipe_do_send`

**File:** `src/sp/transport/ofi/ofi.c` — `ofi_pipe_do_send`

```c
size_t total = hlen + blen;  // no check against OFI_BOUNCE_SZ
NNI_PUT64((uint8_t *) p->tx_buf, (uint64_t) total);
memcpy((uint8_t *) p->tx_buf + 8, nni_msg_header(msg), hlen);
```

If `total + 8 > OFI_BOUNCE_SZ`, the `memcpy` writes past the end of the 1 MiB
bounce buffer, corrupting the heap.

**Fix:** Fail the AIO with `NNG_EMSGSIZE` before any `memcpy` if
`total + 8 > OFI_BOUNCE_SZ`.

---

### C3. RDM shared endpoint/CQ destroyed by individual pipe teardown (architectural)

**Files:** `ofi_listener_bind`, `ofi_pipe_alloc`, `ofi_pipe_fini`,
`ofi_pipe_drain_cqs`

For `FI_EP_RDM`, the listener creates **one** shared endpoint (`ep->ep`) and
shared completion queues (`ep->rdm_tcq`, `ep->rdm_rcq`).  These are handed to
every `ofi_pipe`.  However, `ofi_pipe_fini` unconditionally closes them:

```c
if (p->ep)    fi_close(&p->ep->fid);
if (p->tx_cq) fi_close(&p->tx_cq->fid);
if (p->rx_cq) fi_close(&p->rx_cq->fid);
```

The **first pipe to be torn down destroys the shared resources**, crashing every
other active pipe.  Additionally, every pipe registers the same CQ `wait_fd`
with NNG's poller, creating a race on CQ reads where a completion for one peer
is consumed by the wrong pipe's `rx_buf`.

**Fix:** RDM pipes must not own or close the shared endpoint/CQs.  Two viable
approaches:

1. **Preferred — per-peer endpoint:** allocate a dedicated `fid_ep` per peer in
   the sideband thread (some providers support this; avoids shared ownership).
2. **Alternative — central dispatcher:** keep a single shared endpoint, poll the
   shared CQ in one dispatcher routine, route completions to the correct pipe
   using `fi_addr_t` or `op_context`.

Either way, `ofi_pipe_fini` must be made aware of whether it owns its endpoint.

---

### C4. Unbounded `malloc` from network input in `ofi_sideband_exchange`

**File:** `src/sp/transport/ofi/ofi.c` — `ofi_sideband_exchange`

```c
size_t peer_len = ntohl(peer_len_net);
void *peer_addr = malloc(peer_len);   // attacker controls peer_len
```

A malicious peer sends `0xFFFFFFFF` → 4 GiB allocation → OOM crash, or a
`NULL` return that is not checked before the subsequent `recv()`.

**Fix:** Enforce a strict upper bound (e.g., 4096 bytes), return an error if
exceeded.  Check `malloc` return for NULL.

---

## HIGH — Both Reviewers Agree

### H1. Missing AIO cancellation on pipe close → potential deadlock

**File:** `ofi_pipe_close`

`ofi_pipe_close` sets `p->closed = true` but leaves all pending AIOs in
`sendq` and `recvq` unresolved.  NNG's pipe teardown blocks until all AIOs
complete.  With no mechanism to drain these queues, `nng_close()` can hang
indefinitely.

**Fix:** In `ofi_pipe_close`, iterate over both queues and finish every pending
AIO with `NNG_ECLOSED`.

---

### H2. TX send queue permanently stalls after `fi_sendmsg` error

**File:** `ofi_pipe_do_send`

If `fi_sendmsg` returns an error the current AIO is finished with the error, but
no attempt is made to dispatch the **next** AIO in `sendq`.  Since no CQ
completion will ever fire for the failed operation, the pipe's TX queue is
permanently stalled.

**Fix:** After finishing a failed AIO with an error, loop (or recursively call
`ofi_pipe_do_send`) to dispatch the next queued AIO.

---

### H3. Synchronous blocking I/O in `ofi_dialer_connect` (RDM/CXI path)

**File:** `ofi_dialer_connect`

The CXI sideband path calls `getaddrinfo()`, `connect()`, `send()`, and `recv()`
synchronously on the caller's thread, which is NNG's internal transport thread.
A non-responsive peer will stall the entire connection attempt indefinitely,
violating NNG's asynchronous `d_connect` contract.

**Fix:** Move the sideband TCP exchange to a dedicated background thread
(mirroring the listener's `ofi_rdm_listener_sideband` pattern).

---

### H4. Resource leaks in close/fini paths

Multiple `fi_close` calls and `free`/`nni_strfree` calls are missing:

- `ofi_listener_close` (also used as `l_fini`): does not `fi_close` the passive
  endpoint (`ep->pep`), event queue (`ep->eq`), or address vector (`ep->av`).
- `ofi_dialer_close` (also used as `d_fini`): does not free `ep->dest_addr`
  (`nni_free`) or close `ep->eq` / `ep->av`.
- `ofi_tran_fini`: leaves `ofi_fabric`, `ofi_domain`, and `ofi_base_info`
  allocated forever; `fi_freeinfo(ofi_base_info)` is never called.

These cause libfabric resource exhaustion on every reconnect cycle.

---

### H5. No `fi_cq_readerr` call when CQ returns `-FI_EAVAIL`

**File:** `ofi_pipe_drain_cqs`

When `fi_cq_read` returns a negative value, it may be returning `-FI_EAVAIL`
(error entry available).  Without draining the error entry via `fi_cq_readerr`,
the CQ remains permanently in an error state, blocking all future completions on
that pipe.

**Fix:** Check for `n == -FI_EAVAIL`; call `fi_cq_readerr` to consume the error
entry, log it, and close the pipe.

---

## MEDIUM

### M1. `read(wait_fd, ...)` in drain loop — provider-dependent safety

**File:** `ofi_pipe_drain_cqs`

```c
while (read(wait_fd, buf, sizeof(buf)) > 0) found = true;
```

The `wait_fd` from `FI_WAIT_FD` is an eventfd/pipe on many providers (safe to
`read`), but is an epoll fd or other internal object on others.  Reading from it
directly can steal provider-internal events or cause undefined behavior on
non-`sockets`/`tcp` providers such as CXI.

**Divergence:** Gemini rated this Critical; Claude rated it Medium
(provider-specific).  **Both agree it should be removed.** Rely solely on
`nni_posix_pfd_arm` to re-arm the poller trigger.

---

### M2. `NNI_GET16` macro misuse — confusing syntax

**File:** `ofi_pipe_nego_complete`

```c
p->peer = NNI_GET16(&p->rx_nego[4], p->peer);
```

The NNG `NNI_GET16(ptr, val)` macro assigns to `val` as a side effect but
reads as if it is extracting into a local.  This is functionally correct but
misleading; consider replacing with an explicit big-endian decode.

---

### M3. `ofi_tran_fini` is a no-op — global state never cleaned up

`ofi_fabric`, `ofi_domain`, and `ofi_base_info` are process-lifetime singletons.
`nng_fini()` callers expect a clean shutdown; the global libfabric state should
be torn down in `ofi_tran_fini`.

---

### M4. Test coverage is minimal

The four tests (`ofi-scheme-recognized`, `ofi-listen`, `ofi-connect`,
`ofi-exchange`) cover only the basic happy path.  Missing coverage:

- Oversized message rejection (`NNG_EMSGSIZE`)
- Pipe teardown and reconnection
- Concurrent connections (multiple peers on one listener)
- RDM mode end-to-end
- Sideband address exchange failure paths
- AIO cancellation under load

---

## Positives — Both Reviewers Agree

- **fd-based CQ polling via `nni_posix_pfd`:** Excellent integration with NNG's
  epoll-based event loop.  Avoids the CPU-burning busy-poll typical of RDMA
  transport prototypes.
- **CMake integration:** Clean use of `PkgConfig::OFI` as an imported target,
  correct use of `mark_as_advanced`, and minimal conditional scaffolding.
- **Hybrid MSG/RDM auto-detection:** The fallback from `FI_EP_MSG` to `FI_EP_RDM`
  in `ofi_tran_init` allows the transport to work across very different provider
  types without application changes.
- **SP negotiation wire format:** Reuses the same 8-byte handshake as the TCP
  transport, keeping the protocol layer simple and consistent.
- **Minimal invasive supporting changes:** `marry.c`, `url.c`, `transport.c`, and
  `NNGOptions.cmake` changes are small, correct, and well-scoped.

---

## Divergence Summary

| Topic | Gemini | Claude | Resolution |
|---|---|---|---|
| `read(wait_fd)` severity | Critical | Medium | Both agree: remove it |
| RDM routing fix approach | `fi_cq_data_entry` dispatcher | Per-peer endpoint | Both valid; per-peer EP is simpler for NNG's pipe model |
| Blocking dialer severity | High | High | Full agreement |

---

## Recommended Fix Priority

| Priority | Items | Effort |
|----------|-------|--------|
| **P0 — Before any testing** | C1 RX bounds + alloc check, C2 TX bounds check, C4 sideband malloc cap | Small — validation guards |
| **P1 — Before functional use** | H1 AIO drain on close, H2 TX stall recovery, H4 resource leaks, H5 CQ error drain | Medium — straightforward additions |
| **P2 — Before multi-peer RDM** | C3 shared endpoint architecture refactor | Large — pipe ownership redesign |
| **P3 — Before production** | H3 async sideband, M1 wait_fd read removal, M3 global teardown, M4 test coverage | Medium–Large |

---

## Follow-Up Review (2026-03-01)

After implementing all P0–P3 fixes from the initial review, a second consensus
review was conducted (Claude Opus 4.6 + Gemini 3.1 Pro).  Six new findings
were identified (N1–N6).

### Status of Initial Findings

All original findings (C1–C4, H1–H5, M1–M4) have been addressed in commit
`c80030dc`.  Key implementation details:

- **C1/C2:** TX and RX bounds checks guard against bounce buffer overflow
- **C3:** `owns_ep` flag prevents RDM listener pipes from closing shared resources
- **C4:** 4096-byte upper bound on sideband address exchange + malloc check
- **H1:** AIO drain loop in `ofi_pipe_close` with correct lock discipline
- **H3:** RDM dialer sideband runs in dedicated `nni_thr` background thread
- **H4:** Separate `fini` functions close all libfabric objects
- **H5:** `fi_cq_readerr` called when CQ returns `-FI_EAVAIL`
- **M1:** Direct `read(wait_fd)` removed; rely on `nni_posix_pfd` exclusively
- **M4:** 3 new tests added (large-message, reconnect, concurrent-pipes)

### N1. RDM Shared CQ Multiplexing — KNOWN LIMITATION (not fixed)

**Severity:** Critical for multi-peer RDM; not applicable to single-peer or MSG mode

In RDM listener mode, all pipes share one endpoint (`ep->ep`) and one pair of
CQs (`ep->rdm_tcq`, `ep->rdm_rcq`).  CQ completions for different peers are
interleaved — a completion for pipe A can be consumed by pipe B's poller,
causing data corruption.  Additionally, all pipes register the same `wait_fd`,
so epoll wakeups are broadcast to every pipe.

**Impact:** RDM listener mode is limited to **one peer at a time**.

**Required fix:** Either (1) a central CQ dispatcher that routes completions by
`fi_addr_t` or `op_context`, or (2) per-peer `fid_ep` + CQ allocation in the
sideband thread.  This is a substantial architectural change documented in the
source code comment block above `ofi_rdm_listener_sideband`.

### N2. Dialer Reconnect Deadlock — FIXED

**Severity:** High

`rdm_sideband_started` was permanently `true` after the first sideband thread
launch, blocking NNG's redial mechanism from starting a new thread.

**Fix:** Join the previous thread via `nni_thr_fini` before re-initializing
and running a new one on each connect attempt.

### N3. RDM Dialer Resource Leak — FIXED

**Severity:** High

`p->owns_ep = (ofi_ep_type == FI_EP_MSG)` did not account for RDM dialer pipes,
which each create a fresh EP + CQ pair.  These leaked on every pipe teardown.

**Fix:** `p->owns_ep = is_dialer || (ofi_ep_type == FI_EP_MSG)`.

### N4. Recursive `ofi_pipe_do_send` — FIXED

**Severity:** Low

Error paths recursively called `ofi_pipe_do_send`.  Converted to an iterative
`while` loop with `continue`/`break` to avoid unbounded stack growth.

### N5. Test-Only Observation — DISMISSED

Observation about test architecture; no code change needed.

### N6. OOM Silent Message Drop — FIXED

**Severity:** Medium

When `nni_msg_alloc` failed on the RX path, the code silently re-posted the
receive buffer and continued, discarding the message with no indication to the
sender.

**Fix:** Close the pipe on OOM (`nni_pipe_close`) to trigger proper teardown
rather than silently dropping messages.

---

## Final Assessment

**Status: READY WITH CAVEATS for EXPERIMENTAL use**

All critical, high, and medium findings from both review rounds have been
addressed except N1 (RDM shared CQ multiplexing), which is documented as a
known limitation.  The transport is suitable for:

- FI_EP_MSG providers (tcp, verbs): full multi-peer support
- FI_EP_RDM providers (CXI/Slingshot): single-peer point-to-point only
- Development, testing, and benchmarking of RDMA-accelerated NNG applications

Multi-peer RDM support requires the architectural changes described in N1.
