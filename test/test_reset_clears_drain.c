/*
 * White-box test: fuse_session_reset() clears the controlled-drain state.
 * Loop reuse is outside this test -- it only verifies the drain fields are
 * cleared, not that the session can be looped again.
 *
 * Before the fix fuse_session_reset() reset only mt_exited + error and left the
 * drain fields stale: a left-over recv_paused would make every worker of a
 * later fuse_session_loop_mt() park immediately (a silent hang, no request
 * served), and stale worker_total / exited_workers would skew the
 * fuse_session_wait_drained() predicate. This test seeds those fields the way a
 * loop that paused mid-flight (and was never resumed) would leave them, calls
 * fuse_session_reset(), and asserts they are all back to the fresh baseline. No
 * mount is needed: reset() only touches session state.
 */

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 18)

#include "fuse_i.h"
#include "fuse_lowlevel.h"

#include <assert.h>
#include <err.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>

static struct fuse_session *new_session(void)
{
	const struct fuse_lowlevel_ops ops = { 0 };
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;

	if (fuse_opt_add_arg(&args, "test_reset_clears_drain"))
		errx(1, "Failed to add argument");
	se = fuse_session_new(&args, &ops, sizeof(ops), NULL);
	assert(se);
	fuse_opt_free_args(&args);
	return se;
}

int main(void)
{
	struct fuse_session *se = new_session();
	int fail = 0;

	/* Seed a stale drain generation: paused, with non-zero counters as a
	 * loop that paused mid-flight (and was never resumed) would leave. */
	fuse_session_pause_receive(se); /* recv_paused = true */
	atomic_store_explicit(&se->worker_total, 5, memory_order_relaxed);
	atomic_store_explicit(&se->exited_workers, 4, memory_order_relaxed);
	atomic_store_explicit(&se->parked, 1, memory_order_relaxed);
	atomic_store_explicit(&se->reading, 2, memory_order_relaxed);
	atomic_store_explicit(&se->received_inflight, 3, memory_order_relaxed);

	fuse_session_reset(se);

	/* recv_paused must be cleared: otherwise every worker of the next loop
	 * generation parks immediately and the mount silently hangs. */
	if (atomic_load_explicit(&se->recv_paused, memory_order_seq_cst)) {
		fprintf(stderr, "reset did not clear recv_paused\n");
		fail = 1;
	}

#define CHECK_ZERO(field)                                                 \
	do {                                                              \
		int v = atomic_load_explicit(&se->field,                 \
					     memory_order_relaxed);      \
		if (v != 0) {                                            \
			fprintf(stderr, "reset did not clear " #field    \
					": %d\n", v);                    \
			fail = 1;                                        \
		}                                                        \
	} while (0)
	CHECK_ZERO(worker_total);
	CHECK_ZERO(exited_workers);
	CHECK_ZERO(parked);
	CHECK_ZERO(reading);
	CHECK_ZERO(received_inflight);
#undef CHECK_ZERO

	/* Behavioural corroboration: recv_paused cleared means wait_drained()
	 * rejects with -EINVAL (not paused) instead of reporting a bogus drained
	 * point off the stale counters. */
	if (fuse_session_wait_drained(se, 0) != -EINVAL) {
		fprintf(stderr,
			"after reset the session is not in the unpaused state\n");
		fail = 1;
	}
	/* The public inflight getter agrees the count was cleared. */
	if (fuse_session_received_inflight(se) != 0) {
		fprintf(stderr,
			"received_inflight getter non-zero after reset\n");
		fail = 1;
	}

	fuse_session_destroy(se);
	if (fail)
		return 1;
	printf("reset clears drain state test passed\n");
	return 0;
}
