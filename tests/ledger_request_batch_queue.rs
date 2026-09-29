#[path = "../benches/support/request_batch_queue.rs"]
#[allow(dead_code)]
mod request_batch_queue;

use request_batch_queue::{spawn, Config, FlushReason};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, Semaphore};

fn config(capacity: usize, max_batch_size: usize, timeout: Duration) -> Config {
    Config {
        capacity,
        max_batch_size,
        timeout,
    }
}

fn stage_sum<R>(completed: &request_batch_queue::Completed<R>) -> Duration {
    completed.enqueue_wait
        + completed.queue_wait
        + completed.batch_wait
        + completed.handler_time
        + completed.response_wait
}

fn expect_error<T>(result: Result<T, String>) -> String {
    match result {
        Err(error) => error,
        Ok(_) => panic!("expected the request to fail"),
    }
}

#[tokio::test]
async fn flushes_full_batches_and_preserves_handler_reply_order() {
    let (queue, worker) = spawn(
        config(4, 3, Duration::from_secs(1)),
        |ids: Vec<u64>| async move { Ok(ids.into_iter().map(|id| id * 10).collect()) },
    )
    .unwrap();

    let mut requests = Vec::new();
    for id in 0..3 {
        requests.push(queue.submit(id).await.unwrap());
    }
    let mut replies = Vec::new();
    for request in requests {
        replies.push(request.wait().await.unwrap());
    }

    assert_eq!(
        replies.iter().map(|reply| reply.reply).collect::<Vec<_>>(),
        [0, 10, 20]
    );
    assert!(replies.iter().all(|reply| reply.batch_size == 3));
    assert!(replies
        .iter()
        .all(|reply| reply.flush_reason == FlushReason::Size));
    assert!(replies
        .iter()
        .all(|reply| stage_sum(reply) == reply.total_time));

    drop(queue);
    worker.join().await.unwrap();
}

#[tokio::test]
async fn flushes_partial_batch_when_first_dequeue_deadline_expires() {
    let (queue, worker) = spawn(
        config(4, 4, Duration::from_millis(15)),
        |ids: Vec<u64>| async move { Ok(ids) },
    )
    .unwrap();

    let reply = queue.submit(42).await.unwrap().wait().await.unwrap();
    assert_eq!(reply.reply, 42);
    assert_eq!(reply.batch_size, 1);
    assert_eq!(reply.flush_reason, FlushReason::Timeout);
    assert_eq!(stage_sum(&reply), reply.total_time);

    drop(queue);
    worker.join().await.unwrap();
}

#[tokio::test]
async fn channel_close_flushes_the_partial_tail() {
    let (queue, worker) = spawn(
        config(4, 4, Duration::from_secs(30)),
        |ids: Vec<u64>| async move { Ok(ids) },
    )
    .unwrap();
    let request = queue.submit(7).await.unwrap();
    drop(queue);

    let reply = request.wait().await.unwrap();
    assert_eq!(reply.reply, 7);
    assert_eq!(reply.flush_reason, FlushReason::ChannelClosed);
    assert_eq!(reply.batch_size, 1);
    worker.join().await.unwrap();
}

#[tokio::test]
async fn handler_error_fails_current_and_queued_accepted_requests() {
    let gate = Arc::new(Semaphore::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let handler_gate = Arc::clone(&gate);
    let (queue, worker) = spawn(
        config(4, 1, Duration::from_secs(1)),
        move |_ids: Vec<u64>| {
            let gate = Arc::clone(&handler_gate);
            let started_tx = Arc::clone(&started_tx);
            async move {
                if let Some(started_tx) = started_tx.lock().unwrap().take() {
                    let _ = started_tx.send(());
                }
                gate.acquire().await.unwrap().forget();
                Err::<Vec<u64>, String>("mock handler failure".to_owned())
            }
        },
    )
    .unwrap();

    let first = queue.submit(1).await.unwrap();
    started_rx.await.unwrap();
    let second = queue.submit(2).await.unwrap();
    gate.add_permits(1);

    assert!(expect_error(first.wait().await).contains("mock handler failure"));
    assert!(expect_error(second.wait().await).contains("mock handler failure"));
    assert!(expect_error(queue.submit(3).await).contains("worker stopped"));
    assert!(worker
        .join()
        .await
        .unwrap_err()
        .contains("mock handler failure"));
}

#[tokio::test]
async fn short_handler_reply_fails_batch_and_stops_worker() {
    let (queue, worker) = spawn(
        config(2, 1, Duration::from_secs(1)),
        |_ids: Vec<u64>| async move { Ok(Vec::<u64>::new()) },
    )
    .unwrap();

    let error = expect_error(queue.submit(1).await.unwrap().wait().await);
    assert!(error.contains("returned 0 replies for 1 requests"));
    assert!(worker
        .join()
        .await
        .unwrap_err()
        .contains("returned 0 replies"));
    assert!(expect_error(queue.submit(2).await).contains("worker stopped"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backpressure_blocks_unaccepted_work_and_cancelled_response_is_still_processed() {
    let gate = Arc::new(Semaphore::new(0));
    let processed = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let handler_gate = Arc::clone(&gate);
    let handler_processed = Arc::clone(&processed);
    let (queue, worker) = spawn(
        config(1, 1, Duration::from_secs(1)),
        move |ids: Vec<u64>| {
            let gate = Arc::clone(&handler_gate);
            let processed = Arc::clone(&handler_processed);
            let started_tx = Arc::clone(&started_tx);
            async move {
                if let Some(started_tx) = started_tx.lock().unwrap().take() {
                    let _ = started_tx.send(());
                }
                gate.acquire().await.unwrap().forget();
                processed.fetch_add(ids.len(), Ordering::SeqCst);
                Ok(ids)
            }
        },
    )
    .unwrap();

    let cancelled_response = queue.submit(1).await.unwrap();
    started_rx.await.unwrap();
    let second = queue.submit(2).await.unwrap();
    let blocked_queue = queue.clone();
    let mut third_submit = tokio::spawn(async move { blocked_queue.submit(3).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut third_submit)
            .await
            .is_err()
    );
    third_submit.abort();

    drop(cancelled_response);
    gate.add_permits(2);
    let second_reply = second.wait().await.unwrap();
    assert_eq!(second_reply.reply, 2);
    assert_eq!(processed.load(Ordering::SeqCst), 2);

    drop(queue);
    worker.join().await.unwrap();
}
