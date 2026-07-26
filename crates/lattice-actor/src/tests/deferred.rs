//! Off-turn work: deferred replies, pipe-to-self, and the shared capacity budget.

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, Semaphore};

use super::{ASK_TIMEOUT, DeferredReply, Ping, PipeRecord, ProbePipeCapacity, Record, TestActor};
use crate::{mailbox::MailboxConfig, runtime::spawn_actor};

#[tokio::test]
async fn deferred_reply_does_not_block_the_actor_mailbox() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        TestActor {
            events,
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );
    let reply_gate = Arc::new(Semaphore::new(0));
    let deferred_gate = reply_gate.clone();
    let entered = Arc::new(Semaphore::new(0));
    let deferred_entered = entered.clone();
    let ask_handle = handle.clone();
    let ask = tokio::spawn(async move {
        ask_handle
            .ask(
                DeferredReply {
                    gate: deferred_gate,
                    entered: deferred_entered,
                },
                ASK_TIMEOUT,
            )
            .await
    });

    entered.acquire().await.unwrap().forget();
    let processed = Arc::new(Semaphore::new(0));
    handle
        .tell(Record::with_processed_signal(
            "after-deferred",
            processed.clone(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), processed.acquire())
        .await
        .expect("mailbox should accept the next message while the reply is pending")
        .unwrap()
        .forget();
    assert!(!ask.is_finished());

    // The deferred task owns the ask sender and can answer after the handler returned.
    reply_gate.add_permits(1);
    assert_eq!(ask.await.unwrap().unwrap(), "done");
}

#[tokio::test]
async fn pipe_to_self_posts_async_results_from_a_regular_handler() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        TestActor {
            events: events.clone(),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let pipe_processed = Arc::new(Semaphore::new(0));
    handle
        .tell(PipeRecord {
            gate: gate.clone(),
            entered: entered.clone(),
            processed: pipe_processed.clone(),
        })
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();

    let interleaved = Arc::new(Semaphore::new(0));
    handle
        .tell(Record::with_processed_signal(
            "while-pipe-pending",
            interleaved.clone(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), interleaved.acquire())
        .await
        .expect("mailbox should continue while pipe-to-self work is pending")
        .unwrap()
        .forget();

    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), pipe_processed.acquire())
        .await
        .expect("pipe-to-self result should return through the mailbox")
        .unwrap()
        .forget();
    handle.ask(Ping("barrier"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(
        *events.lock().await,
        vec!["while-pipe-pending", "piped", "barrier"]
    );
}

#[tokio::test]
async fn pipe_to_self_enforces_deferred_operation_capacity() {
    let handle = spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::bounded(8).with_deferred_capacity(1),
    );
    let gate = Arc::new(Semaphore::new(0));

    assert!(
        handle
            .ask(ProbePipeCapacity { gate: gate.clone() }, ASK_TIMEOUT)
            .await
            .unwrap()
    );
    gate.add_permits(1);
}
