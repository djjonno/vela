//! Example test for leader-directed requests (task 16.4, Requirement 11.1).
//!
//! Requirement 11.1: "WHEN a Producer or Consumer issues a request for a
//! Partition, THE client library SHALL direct the request to the Node identified
//! as the current Leader of that Partition."
//!
//! [`ClientCore::dispatch`] is the seam every partition request flows through: it
//! resolves the partition's *believed* leader (the cached leader address, or a
//! `FindLeader` lookup on a miss) and invokes the per-attempt operation against
//! that address. These tests seed the leader cache with a known leader, then
//! dispatch an operation that records the address it was handed, and assert the
//! recorded address is exactly the cached leader — i.e. the request was directed
//! to the node believed to lead the partition.

use std::sync::{Arc, Mutex};

use vela_client::{ClientCore, Result};

/// A bootstrapped core with two known nodes. The addresses double as the
/// registry seed, so cached leader addresses resolve without any network call.
fn core() -> ClientCore {
    ClientCore::new([
        ("node-a".to_string(), "http://node-a:50051".to_string()),
        ("node-b".to_string(), "http://node-b:50051".to_string()),
    ])
}

/// A partition request is directed to the node cached as that partition's leader
/// (Requirement 11.1).
#[tokio::test]
async fn request_is_directed_to_the_cached_leader() {
    let core = core();
    // The client believes node-b leads orders/2.
    core.leaders().insert("orders", 2, "http://node-b:50051");

    // Record the address dispatch hands the operation.
    let directed_to: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let recorder = Arc::clone(&directed_to);

    let result: Result<()> = core
        .dispatch("orders", 2, move |addr| {
            let recorder = Arc::clone(&recorder);
            async move {
                *recorder.lock().expect("recorder mutex poisoned") = Some(addr);
                Ok(())
            }
        })
        .await;

    result.expect("dispatch succeeds against the believed leader");

    // The request reached the cached leader node, not some other node.
    assert_eq!(
        directed_to
            .lock()
            .expect("recorder mutex poisoned")
            .as_deref(),
        Some("http://node-b:50051"),
        "the request was directed to the cached leader of orders/2 (Req 11.1)",
    );
}

/// The directed node tracks the cache per `(topic, partition)`: each partition's
/// request goes to that partition's own cached leader, not a fixed node.
#[tokio::test]
async fn each_partition_is_directed_to_its_own_cached_leader() {
    let core = core();
    core.leaders().insert("orders", 0, "http://node-a:50051");
    core.leaders().insert("orders", 1, "http://node-b:50051");

    async fn directed_addr(core: &ClientCore, partition: u32) -> String {
        let directed_to: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let recorder = Arc::clone(&directed_to);
        let result: Result<()> = core
            .dispatch("orders", partition, move |addr| {
                let recorder = Arc::clone(&recorder);
                async move {
                    *recorder.lock().expect("recorder mutex poisoned") = Some(addr);
                    Ok(())
                }
            })
            .await;
        result.expect("dispatch succeeds against the believed leader");
        let addr = directed_to.lock().expect("recorder mutex poisoned").clone();
        addr.expect("operation was invoked with the leader address")
    }

    // partition 0 is led by node-a, partition 1 by node-b.
    assert_eq!(directed_addr(&core, 0).await, "http://node-a:50051");
    assert_eq!(directed_addr(&core, 1).await, "http://node-b:50051");
}
