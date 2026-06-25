/*
 * White-box test for fuse_session_wait_drained() argument validation and the
 * "never busy-spin on a bad deadline" guarantee (reviewer fix).
 *
 * Before the fix a negative timeout_ms produced a negative tv_nsec, so
 * pthread_cond_timedwait() returned EINVAL, which the loop swallowed into rc=0
 * and re-evaluated immediately -> a tight busy-spin that never returns. The fix
 * rejects timeout_ms < 0 up front with -EINVAL and, for any other non-ETIMEDOUT
 * error, returns the negative errno instead of looping.
 *
 * This test needs no mount: wait_drained() validates the argument before
 * touching any worker state, so a bare session created with fuse_session_new()
 * is enough. To prove "no busy-spin / no hang", the call is driven from a
 * worker thread that the main thread joins with a short deadline via
 * pthread_timedjoin_np(); a spinning/hung implementation would miss the
 * deadline and the test reports failure instead of wedging.
 */

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 18)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  /* pthread_timedjoin_np */
#endif

#include "fuse_i.h"
#include "fuse_lowlevel.h"

#include <assert.h>
#include <err.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

static struct fuse_session *new_session(void)
{
	const struct fuse_lowlevel_ops ops = { 0 };
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;

	if (fuse_opt_add_arg(&args, "test_wait_drained_timeout"))
		errx(1, "Failed to add argument");
	se = fuse_session_new(&args, &ops, sizeof(ops), NULL);
	assert(se);
	fuse_opt_free_args(&args);
	return se;
}

struct call_arg {
	struct fuse_session *se;
	int timeout_ms;
	int rc;
};

static void *call_thread(void *arg)
{
	struct call_arg *ca = arg;

	ca->rc = fuse_session_wait_drained(ca->se, ca->timeout_ms);
	return NULL;
}

/* Run wait_drained(timeout_ms) on a worker thread; require it to return within
 * watchdog_ms. Returns the call's rc, or sets *timed_out on watchdog expiry. */
static int call_bounded(struct fuse_session *se, int timeout_ms,
			int watchdog_ms, int *timed_out)
{
	struct call_arg ca = { .se = se, .timeout_ms = timeout_ms, .rc = 0 };
	pthread_t tid;
	struct timespec deadline;
	int jrc;

	*timed_out = 0;
	if (pthread_create(&tid, NULL, call_thread, &ca))
		errx(1, "pthread_create failed");

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += watchdog_ms / 1000;
	deadline.tv_nsec += (long)(watchdog_ms % 1000) * 1000000L;
	if (deadline.tv_nsec >= 1000000000L) {
		deadline.tv_sec += 1;
		deadline.tv_nsec -= 1000000000L;
	}

	jrc = pthread_timedjoin_np(tid, NULL, &deadline);
	if (jrc == ETIMEDOUT) {
		*timed_out = 1;
		/* Do not join (it may be spinning/hung); leak the thread and
		 * let the process exit report failure. */
		return 0;
	}
	if (jrc != 0)
		errx(1, "pthread_timedjoin_np failed: %d", jrc);
	return ca.rc;
}

int main(void)
{
	struct fuse_session *se = new_session();
	int timed_out, rc;

	/* Negative timeout must be rejected with -EINVAL, promptly (the bug made
	 * this busy-spin forever). A generous 2s watchdog catches a spin/hang. */
	rc = call_bounded(se, -1, 2000, &timed_out);
	if (timed_out) {
		fprintf(stderr,
			"REGRESSION: wait_drained(-1) did not return (busy-spin "
			"or hang)\n");
		return 1;
	}
	if (rc != -EINVAL) {
		fprintf(stderr, "wait_drained(-1) returned %d, expected -EINVAL\n",
			rc);
		return 1;
	}

	/* A large negative value takes the same up-front reject path. */
	rc = call_bounded(se, -1000000, 2000, &timed_out);
	if (timed_out || rc != -EINVAL) {
		fprintf(stderr,
			"wait_drained(large negative) timed_out=%d rc=%d "
			"(expected -EINVAL)\n", timed_out, rc);
		return 1;
	}

	/*
	 * Not paused: wait_drained() must reject with -EINVAL even though the
	 * drained predicate is trivially true on a freshly created session (no
	 * workers -> worker_total==0, reading==0, inflight==0). Without the
	 * recv_paused guard this would wrongly return 0 and look like a safe
	 * handoff point. (Bounded so a regression that waits/spins is caught.)
	 */
	rc = call_bounded(se, 0, 2000, &timed_out);
	if (timed_out) {
		fprintf(stderr, "REGRESSION: wait_drained(0) did not return\n");
		return 1;
	}
	if (rc != -EINVAL) {
		fprintf(stderr,
			"wait_drained(0) on an unpaused session returned %d, "
			"expected -EINVAL\n", rc);
		return 1;
	}

	/*
	 * Paused: once fuse_session_pause_receive() is in effect, the same empty
	 * session is legitimately drained (worker_total==0, reading==0,
	 * inflight==0) and wait_drained() returns 0 promptly.
	 */
	fuse_session_pause_receive(se);
	rc = call_bounded(se, 0, 2000, &timed_out);
	if (timed_out) {
		fprintf(stderr, "REGRESSION: wait_drained(0) (paused) did not return\n");
		return 1;
	}
	if (rc != 0) {
		fprintf(stderr,
			"wait_drained(0) on a paused empty session returned %d, "
			"expected 0\n", rc);
		return 1;
	}

	fuse_session_destroy(se);
	printf("wait_drained timeout-validation test passed\n");
	return 0;
}
