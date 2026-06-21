//! Unit test for the `Cluster::await_ready` startup-budget timeout
//! (Requirement 9.3).
//!
//! The concrete polling-with-deadline behavior lives inside
//! `InProcessCluster::await_ready`, but it can only be exercised by starting a
//! real in-process server — there is no public constructor that points the
//! probe at a port nobody serves. So this test pins the *contract* the trait
//! promises instead: a `Cluster` that never becomes ready must surface
//! [`BenchError::ClusterNotReady`] carrying the configured budget once that
//! budget elapses, and it must return promptly rather than hang.
//!
//! A `NeverReady` fake implements `Cluster` with the same fixed-interval
//! poll-until-deadline loop the real implementation uses, but with a readiness
//! probe that always fails. The test runs under a paused clock
//! (`start_paused = true`) so the budget elapses deterministically and
//! instantly, with no wall-clock wait. A companion `ReadyAfter` fake — ready
//! once a probe count is reached — guards against the loop simply always
//! erroring, so the timeout assertion stays meaningful.

use std::time::Duration;

use async_trait::async_trait;
use vela_bench::cluster::Cluster;
use vela_bench::BenchError;

/// Interval between readiness probes, mirroring the real implementation's fixed
/// poll cadence. Under a paused clock these sleeps auto-advance, so the loop
/// reaches its deadline without any real delay.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A fake [`Cluster`] whose readiness probe never succeeds.
///
/// Its `await_ready` is the same poll-until-deadline shape as
/// [`vela_bench::cluster::InProcessCluster`]: probe, then check the deadline,
/// then sleep — so a never-ready cluster must exit via the deadline branch with
/// [`BenchError::ClusterNotReady`].
struct NeverReady;

#[async_trait]
impl Cluster for NeverReady {
    fn bootstrap(&self) -> Vec<(String, String)> {
        vec![("never-ready".to_string(), "http://127.0.0.1:0".to_string())]
    }

    async fn await_ready(&self, budget: Duration) -> Result<(), BenchError> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            // The probe never reports the cluster as ready.
            if tokio::time::Instant::now() >= deadline {
                return Err(BenchError::ClusterNotReady {
                    budget_secs: budget.as_secs(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn shutdown(self) -> Result<(), BenchError> {
        Ok(())
    }
}

/// A fake [`Cluster`] that becomes ready after a fixed number of probes, well
/// within the budget — the positive counterpart that keeps the timeout test
/// honest (a loop that always errored would fail this one).
struct ReadyAfter {
    probes_until_ready: u32,
}

#[async_trait]
impl Cluster for ReadyAfter {
    fn bootstrap(&self) -> Vec<(String, String)> {
        vec![("ready-after".to_string(), "http://127.0.0.1:0".to_string())]
    }

    async fn await_ready(&self, budget: Duration) -> Result<(), BenchError> {
        let deadline = tokio::time::Instant::now() + budget;
        let mut probes = 0u32;
        loop {
            if probes >= self.probes_until_ready {
                return Ok(());
            }
            probes += 1;
            if tokio::time::Instant::now() >= deadline {
                return Err(BenchError::ClusterNotReady {
                    budget_secs: budget.as_secs(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn shutdown(self) -> Result<(), BenchError> {
        Ok(())
    }
}

/// A cluster that never becomes ready errors with `ClusterNotReady`, carrying
/// the configured startup budget, once that budget elapses (Requirement 9.3).
#[tokio::test(start_paused = true)]
async fn await_ready_times_out_when_cluster_never_ready() {
    let budget = Duration::from_secs(2);

    let result = NeverReady.await_ready(budget).await;

    match result {
        Err(BenchError::ClusterNotReady { budget_secs }) => {
            assert_eq!(
                budget_secs,
                budget.as_secs(),
                "the reported budget reflects the configured startup budget"
            );
        }
        other => panic!("expected Err(ClusterNotReady), got {other:?}"),
    }
}

/// The timeout is driven by actual non-readiness, not an unconditional error: a
/// cluster that becomes ready within the budget resolves `Ok`.
#[tokio::test(start_paused = true)]
async fn await_ready_resolves_ok_when_cluster_becomes_ready_in_budget() {
    let budget = Duration::from_secs(60);

    let result = ReadyAfter {
        probes_until_ready: 3,
    }
    .await_ready(budget)
    .await;

    assert!(
        result.is_ok(),
        "a cluster that becomes ready within the budget does not time out, got {result:?}"
    );
}
