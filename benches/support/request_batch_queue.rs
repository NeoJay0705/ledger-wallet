//! A single-worker bounded Tokio mpsc queue that invokes handlers on batches.
//!
//! The handler owns each batch and returns replies in the same order. Accepted
//! requests continue to be processed if their response receiver is dropped.
//! Handler failure or an invalid reply count fails the current batch and every
//! request still queued, then closes the worker coherently.

use std::future::Future;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlushReason {
    Size,
    Timeout,
    ChannelClosed,
}

impl FlushReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Timeout => "timeout",
            Self::ChannelClosed => "channel_closed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub capacity: usize,
    pub max_batch_size: usize,
    pub timeout: Duration,
}

impl Config {
    pub fn validate(self) -> Result<Self, String> {
        if self.capacity == 0 {
            return Err("request queue capacity must be greater than zero".to_owned());
        }
        if self.max_batch_size == 0 {
            return Err("maximum batch size must be greater than zero".to_owned());
        }
        if self.timeout.is_zero() {
            return Err("batch timeout must be greater than zero".to_owned());
        }
        Ok(self)
    }
}

pub struct BatchQueue<P, R> {
    sender: mpsc::Sender<Envelope<P, R>>,
}

impl<P, R> Clone for BatchQueue<P, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

pub struct BatchWorker {
    task: JoinHandle<Result<(), String>>,
}

pub struct RequestHandle<R> {
    receiver: oneshot::Receiver<Result<WorkerReply<R>, String>>,
    enqueue_started_at: Instant,
    accepted_at: Instant,
}

pub struct Completed<R> {
    pub reply: R,
    pub request_started_at: Instant,
    pub response_observed_at: Instant,
    pub total_time: Duration,
    pub enqueue_wait: Duration,
    pub queue_wait: Duration,
    pub batch_wait: Duration,
    pub handler_time: Duration,
    pub response_wait: Duration,
    pub batch_size: usize,
    pub flush_reason: FlushReason,
}

struct Envelope<P, R> {
    payload: P,
    accepted_at: Instant,
    response: oneshot::Sender<Result<WorkerReply<R>, String>>,
}

struct WorkerReply<R> {
    reply: R,
    accepted_at: Instant,
    dequeued_at: Instant,
    handler_started_at: Instant,
    handler_finished_at: Instant,
    batch_size: usize,
    flush_reason: FlushReason,
}

struct Pending<P, R> {
    payload: Option<P>,
    accepted_at: Instant,
    dequeued_at: Instant,
    response: oneshot::Sender<Result<WorkerReply<R>, String>>,
}

/// Start a bounded batch queue and its sole worker.
pub fn spawn<P, R, F, Fut>(
    config: Config,
    handler: F,
) -> Result<(BatchQueue<P, R>, BatchWorker), String>
where
    P: Send + 'static,
    R: Send + 'static,
    F: Fn(Vec<P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<R>, String>> + Send + 'static,
{
    let config = config.validate()?;
    let (sender, receiver) = mpsc::channel(config.capacity);
    let task = tokio::spawn(run_worker(receiver, config, handler));
    Ok((BatchQueue { sender }, BatchWorker { task }))
}

impl<P, R> BatchQueue<P, R>
where
    P: Send + 'static,
    R: Send + 'static,
{
    /// Wait for bounded-channel capacity, then publish one accepted request.
    /// The returned handle may be dropped without canceling accepted work.
    pub async fn submit(&self, payload: P) -> Result<RequestHandle<R>, String> {
        let enqueue_started_at = Instant::now();
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| "batch worker stopped before request admission".to_owned())?;
        // This timestamp deliberately precedes the synchronous permit.send().
        // Queue wait therefore includes publication and cannot become negative
        // if the worker dequeues immediately on another runtime thread.
        let accepted_at = Instant::now();
        let (response, receiver) = oneshot::channel();
        permit.send(Envelope {
            payload,
            accepted_at,
            response,
        });

        Ok(RequestHandle {
            receiver,
            enqueue_started_at,
            accepted_at,
        })
    }
}

impl<R> RequestHandle<R> {
    pub async fn wait(self) -> Result<Completed<R>, String> {
        let Self {
            receiver,
            enqueue_started_at,
            accepted_at,
        } = self;
        let worker_reply = receiver.await.map_err(|_| {
            "batch worker stopped before replying to an accepted request".to_owned()
        })??;
        let response_observed_at = Instant::now();
        let total_time = checked_elapsed(
            response_observed_at,
            enqueue_started_at,
            "end-to-end request",
        )?;

        Ok(Completed {
            reply: worker_reply.reply,
            request_started_at: enqueue_started_at,
            response_observed_at,
            total_time,
            enqueue_wait: checked_elapsed(accepted_at, enqueue_started_at, "enqueue")?,
            queue_wait: checked_elapsed(
                worker_reply.dequeued_at,
                worker_reply.accepted_at,
                "queue wait",
            )?,
            batch_wait: checked_elapsed(
                worker_reply.handler_started_at,
                worker_reply.dequeued_at,
                "batch wait",
            )?,
            handler_time: checked_elapsed(
                worker_reply.handler_finished_at,
                worker_reply.handler_started_at,
                "handler",
            )?,
            response_wait: checked_elapsed(
                response_observed_at,
                worker_reply.handler_finished_at,
                "response wait",
            )?,
            batch_size: worker_reply.batch_size,
            flush_reason: worker_reply.flush_reason,
        })
    }
}

fn checked_elapsed(later: Instant, earlier: Instant, stage: &str) -> Result<Duration, String> {
    later
        .checked_duration_since(earlier)
        .ok_or_else(|| format!("{stage} timing moved backwards"))
}

impl BatchWorker {
    /// Join after dropping every `BatchQueue` sender so the channel can close.
    pub async fn join(self) -> Result<(), String> {
        self.task.await.map_err(|error| {
            if error.is_panic() {
                format!("batch worker panicked: {error}")
            } else if error.is_cancelled() {
                "batch worker was cancelled".to_owned()
            } else {
                format!("batch worker join failed: {error}")
            }
        })?
    }
}

async fn run_worker<P, R, F, Fut>(
    mut receiver: mpsc::Receiver<Envelope<P, R>>,
    config: Config,
    handler: F,
) -> Result<(), String>
where
    P: Send + 'static,
    R: Send + 'static,
    F: Fn(Vec<P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<R>, String>> + Send + 'static,
{
    loop {
        let first = match receiver.recv().await {
            Some(request) => request,
            None => return Ok(()),
        };

        let first_dequeued_at = Instant::now();
        let Some(deadline) = first_dequeued_at.checked_add(config.timeout) else {
            let error = "batch timeout exceeds the monotonic clock range".to_owned();
            fail_batch_and_drain(
                vec![Pending {
                    payload: Some(first.payload),
                    accepted_at: first.accepted_at,
                    dequeued_at: first_dequeued_at,
                    response: first.response,
                }],
                &mut receiver,
                &error,
            )
            .await;
            return Err(error);
        };
        let mut batch = vec![Pending {
            payload: Some(first.payload),
            accepted_at: first.accepted_at,
            dequeued_at: first_dequeued_at,
            response: first.response,
        }];
        let flush_reason = loop {
            if batch.len() >= config.max_batch_size {
                break FlushReason::Size;
            }

            match timeout_at(deadline.into(), receiver.recv()).await {
                Ok(Some(request)) => {
                    let dequeued_at = Instant::now();
                    batch.push(Pending {
                        payload: Some(request.payload),
                        accepted_at: request.accepted_at,
                        dequeued_at,
                        response: request.response,
                    });
                    if batch.len() >= config.max_batch_size {
                        break FlushReason::Size;
                    }
                }
                Ok(None) => break FlushReason::ChannelClosed,
                Err(_) => break FlushReason::Timeout,
            }
        };

        let batch_size = batch.len();
        let payloads = batch
            .iter_mut()
            .map(|request| request.payload.take().expect("pending payload exists"))
            .collect::<Vec<_>>();

        let handler_started_at = Instant::now();
        let handler_result = handler(payloads).await;
        let handler_finished_at = Instant::now();

        let replies = match handler_result {
            Ok(replies) if replies.len() == batch_size => replies,
            Ok(replies) => {
                let error = format!(
                    "batch handler returned {} replies for {batch_size} requests",
                    replies.len()
                );
                fail_batch_and_drain(batch, &mut receiver, &error).await;
                return Err(error);
            }
            Err(error) => {
                fail_batch_and_drain(batch, &mut receiver, &error).await;
                return Err(format!("batch handler failed: {error}"));
            }
        };

        for (request, reply) in batch.into_iter().zip(replies) {
            let _ = request.response.send(Ok(WorkerReply {
                reply,
                accepted_at: request.accepted_at,
                dequeued_at: request.dequeued_at,
                handler_started_at,
                handler_finished_at,
                batch_size,
                flush_reason,
            }));
        }

        if flush_reason == FlushReason::ChannelClosed {
            return Ok(());
        }
    }
}

async fn fail_batch_and_drain<P, R>(
    batch: Vec<Pending<P, R>>,
    receiver: &mut mpsc::Receiver<Envelope<P, R>>,
    error: &str,
) {
    for request in batch {
        let _ = request.response.send(Err(error.to_owned()));
    }
    receiver.close();
    while let Some(request) = receiver.recv().await {
        let _ = request.response.send(Err(error.to_owned()));
    }
}
