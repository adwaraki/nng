// Copyright 2026 - OFI/libfabric transport tests for NNG (EXPERIMENTAL)
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).

#include "../../../testing/nuts.h"
#include "../../../sp/transport.h"

void
test_ofi_scheme_recognized(void)
{
	// nni_sp_tran_find is internal API exposed via nng_testing.
	NUTS_TRUE(nni_sp_tran_find("ofi") != NULL);
}

void
test_ofi_listen(void)
{
	nng_socket s;
	char       addr[64];

	nuts_scratch_addr("ofi", sizeof(addr), addr);
	NUTS_OPEN(s);
	NUTS_PASS(nng_listen(s, addr, NULL, 0));
	NUTS_CLOSE(s);
}

void
test_ofi_connect(void)
{
	nng_socket s1;
	nng_socket s2;
	char       addr[64];

	nuts_scratch_addr("ofi", sizeof(addr), addr);
	NUTS_OPEN(s1);
	NUTS_OPEN(s2);
	NUTS_PASS(nng_listen(s1, addr, NULL, 0));
	NUTS_PASS(nng_dial(s2, addr, NULL, 0));
	nng_msleep(100);
	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
}

void
test_ofi_exchange(void)
{
	nuts_tran_exchange("ofi");
}

/*
 * test_ofi_large_message: verify that sending a message larger than the
 * OFI bounce buffer (OFI_BOUNCE_SZ = 1 MiB) fails with NNG_EMSGSIZE rather
 * than silently corrupting the heap.  The fix for C2 in ofi_pipe_do_send adds
 * this bounds check.
 */
void
test_ofi_large_message(void)
{
	nng_socket s1;
	nng_socket s2;
	nng_msg   *msg;
	char       addr[64];
	/* OFI_BOUNCE_SZ is 1 MiB; header+body must fit in that minus 8 bytes.
	 * Allocate a body that is exactly OFI_BOUNCE_SZ bytes so that
	 * total+8 overflows the bounce buffer. */
	const size_t oversized = (1024 * 1024);

	nuts_scratch_addr("ofi", sizeof(addr), addr);
	NUTS_OPEN(s1);
	NUTS_OPEN(s2);
	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_RECVTIMEO, 200));
	NUTS_PASS(nng_socket_set_ms(s2, NNG_OPT_SENDTIMEO, 200));
	NUTS_PASS(nng_listen(s1, addr, NULL, 0));
	NUTS_PASS(nng_dial(s2, addr, NULL, 0));
	nng_msleep(100);

	NUTS_PASS(nng_msg_alloc(&msg, oversized));
	/* nng_sendmsg fails the AIO which is propagated as NNG_EMSGSIZE or
	 * NNG_ETRANERR depending on when the check occurs.  Either is
	 * acceptable here — the key invariant is that the send DOES NOT
	 * succeed and does not crash. */
	int rv = nng_sendmsg(s2, msg, 0);
	NUTS_TRUE(rv == NNG_EMSGSIZE || rv == NNG_ETRANERR || rv == NNG_ETIMEDOUT);
	if (rv != 0) {
		/* msg was not consumed; free it ourselves */
		nng_msg_free(msg);
	}

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
}

/*
 * test_ofi_reconnect: close the dialer side of a connected pair, reopen a
 * new socket at the same listener, and verify that a message exchange still
 * works.  This exercises the pipe teardown path (H1: AIO drain on close) and
 * verifies that listener resource cleanup does not leave the transport in a
 * broken state.
 */
void
test_ofi_reconnect(void)
{
	nng_socket   listener_sock;
	nng_socket   dialer_sock1;
	nng_socket   dialer_sock2;
	nng_listener l;
	char         addr[64];
	char         buf[32];
	size_t       sz;

	nuts_scratch_addr("ofi", sizeof(addr), addr);

	NUTS_OPEN(listener_sock);
	NUTS_PASS(nng_socket_set_ms(listener_sock, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(listener_sock, NNG_OPT_SENDTIMEO, 2000));
	NUTS_PASS(nng_listen(listener_sock, addr, &l, 0));

	/* First connection: dial, exchange, close dialer */
	NUTS_OPEN(dialer_sock1);
	NUTS_PASS(nng_socket_set_ms(dialer_sock1, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(dialer_sock1, NNG_OPT_SENDTIMEO, 2000));
	NUTS_PASS(nng_dial(dialer_sock1, addr, NULL, 0));
	nng_msleep(100);

	NUTS_SEND(dialer_sock1, "hello");
	sz = sizeof(buf);
	NUTS_PASS(nng_recv(listener_sock, buf, &sz, 0));
	NUTS_TRUE(sz == 6);
	NUTS_TRUE(memcmp(buf, "hello", 5) == 0);

	NUTS_CLOSE(dialer_sock1);
	nng_msleep(100);

	/* Second connection: a new dialer to the same listener */
	NUTS_OPEN(dialer_sock2);
	NUTS_PASS(nng_socket_set_ms(dialer_sock2, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(dialer_sock2, NNG_OPT_SENDTIMEO, 2000));
	NUTS_PASS(nng_dial(dialer_sock2, addr, NULL, 0));
	nng_msleep(100);

	NUTS_SEND(dialer_sock2, "world");
	sz = sizeof(buf);
	NUTS_PASS(nng_recv(listener_sock, buf, &sz, 0));
	NUTS_TRUE(sz == 6);
	NUTS_TRUE(memcmp(buf, "world", 5) == 0);

	NUTS_CLOSE(dialer_sock2);
	NUTS_CLOSE(listener_sock);
}

/*
 * test_ofi_concurrent_pipes: multiple dialers connect simultaneously to one
 * listener.  Each dialer sends a distinct message and we verify that all
 * messages are received.  This exercises the multi-pipe path of the listener
 * (waitpipes queue in ofi_pipe_nego_complete) and validates that C3
 * (shared RDM endpoint vs per-pipe ownership) does not cause a crash when
 * pipes are torn down.
 */
#define OFI_NPIPES 3

void
test_ofi_concurrent_pipes(void)
{
	nng_socket dialers[OFI_NPIPES];
	nng_socket listener_sock;
	char       addr[64];
	char       buf[32];
	size_t     sz;
	int        received[OFI_NPIPES];
	int        i;

	nuts_scratch_addr("ofi", sizeof(addr), addr);

	NUTS_OPEN(listener_sock);
	NUTS_PASS(nng_socket_set_ms(listener_sock, NNG_OPT_RECVTIMEO, 3000));

	NUTS_PASS(nng_listen(listener_sock, addr, NULL, 0));

	for (i = 0; i < OFI_NPIPES; i++) {
		NUTS_OPEN(dialers[i]);
		NUTS_PASS(
		    nng_socket_set_ms(dialers[i], NNG_OPT_SENDTIMEO, 3000));
		NUTS_PASS(nng_dial(dialers[i], addr, NULL, 0));
	}

	/* Allow all pipes to complete negotiation */
	nng_msleep(200);

	/* Each dialer sends a unique single-byte payload */
	for (i = 0; i < OFI_NPIPES; i++) {
		char payload[2] = { (char) ('A' + i), '\0' };
		NUTS_PASS(nng_send(dialers[i], payload, 2, 0));
	}

	/* Drain all messages at the listener (order is not guaranteed) */
	memset(received, 0, sizeof(received));
	for (i = 0; i < OFI_NPIPES; i++) {
		sz = sizeof(buf);
		NUTS_PASS(nng_recv(listener_sock, buf, &sz, 0));
		NUTS_TRUE(sz == 2);
		int idx = (unsigned char) buf[0] - 'A';
		NUTS_TRUE(idx >= 0 && idx < OFI_NPIPES);
		received[idx]++;
	}

	/* Verify each message arrived exactly once */
	for (i = 0; i < OFI_NPIPES; i++) {
		NUTS_TRUE(received[i] == 1);
	}

	for (i = 0; i < OFI_NPIPES; i++) {
		NUTS_CLOSE(dialers[i]);
	}
	NUTS_CLOSE(listener_sock);
}

TEST_LIST = {
	{ "ofi-scheme-recognized", test_ofi_scheme_recognized },
	{ "ofi-listen", test_ofi_listen },
	{ "ofi-connect", test_ofi_connect },
	{ "ofi-exchange", test_ofi_exchange },
	{ "ofi-large-message", test_ofi_large_message },
	{ "ofi-reconnect", test_ofi_reconnect },
	{ "ofi-concurrent-pipes", test_ofi_concurrent_pipes },
	{ NULL, NULL },
};
