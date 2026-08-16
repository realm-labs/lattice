//! Mailbox delivery: typed ask/tell, lane priority, and bounded-capacity backpressure.

use std::sync::Arc;

use lattice_core::service_context::ServiceContext;
use tokio::sync::{Mutex, Semaphore};

use super::{ASK_TIMEOUT, Ping, Record, TestActor};
use crate::{
    error::ActorTellError,
    mailbox::MailboxConfig,
    runtime::{
        ActorExecutionPolicy, ActorRuntime, ActorSpawnOptions, PassivationPolicy, spawn_actor,
    },
};

#[tokio::test]
async fn actor_handle_ask_and_tell_deliver_typed_messages() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    let reply = handle.ask(Ping("one"), ASK_TIMEOUT).await.unwrap();
    handle.tell(Record::new("two")).await.unwrap();
    let barrier = handle.ask(Ping("barrier"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:one");
    assert_eq!(barrier, "pong:barrier");
    assert_eq!(*events.lock().await, vec!["one", "two", "barrier"]);
}

#[tokio::test]
async fn keyed_worker_pool_system_mailbox_keeps_priority_over_normal_mailbox() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let start_gate = Arc::new(Semaphore::new(0));
    let processed = Arc::new(Semaphore::new(0));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
                start_gate: Some(start_gate.clone()),
                stopped: None,
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                #[cfg(feature = "distributed")]
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .unwrap();

    handle
        .try_tell_for_test(Record::with_processed_signal("normal", processed.clone()))
        .unwrap();
    handle
        .try_tell_system_for_test(Record::with_processed_signal("system", processed.clone()))
        .unwrap();
    start_gate.add_permits(1);
    processed.acquire_many(2).await.unwrap().forget();

    assert_eq!(*events.lock().await, vec!["system", "normal"]);
}

#[tokio::test]
async fn system_mailbox_has_priority_over_normal_mailbox() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let start_gate = Arc::new(Semaphore::new(0));
    let processed = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: events.clone(),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    handle
        .try_tell_for_test(Record::with_processed_signal("normal", processed.clone()))
        .unwrap();
    handle
        .try_tell_system_for_test(Record::with_processed_signal("system", processed.clone()))
        .unwrap();
    start_gate.add_permits(1);
    processed.acquire_many(2).await.unwrap().forget();

    assert_eq!(*events.lock().await, vec!["system", "normal"]);
}

#[tokio::test]
async fn mailbox_full_returns_explicit_error() {
    let start_gate = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: Arc::new(Mutex::new(Vec::new())),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(1));

    handle.try_tell_for_test(Record::new("first")).unwrap();
    let second = handle.try_tell_for_test(Record::new("second"));

    let returned = match second {
        Err(ActorTellError::MailboxFull(message)) => message,
        other => panic!("expected full mailbox, got {other:?}"),
    };
    assert_eq!(returned.value, "second");
    start_gate.add_permits(1);
}

#[tokio::test]
async fn tell_waits_for_capacity_without_losing_the_message() {
    let start_gate = Arc::new(Semaphore::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(1));

    handle.try_tell(Record::new("first")).unwrap();
    let processed = Arc::new(Semaphore::new(0));
    let pending_handle = handle.clone();
    let pending_processed = processed.clone();
    let pending = tokio::spawn(async move {
        pending_handle
            .tell(Record::with_processed_signal("second", pending_processed))
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !pending.is_finished(),
        "tell must wait while the bounded mailbox is full"
    );

    start_gate.add_permits(1);
    pending.await.unwrap().unwrap();
    processed.acquire().await.unwrap().forget();
    assert_eq!(*events.lock().await, vec!["first", "second"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contended_tells_wait_for_capacity_without_losing_messages() {
    const WAITERS: usize = 16;

    let start_gate = Arc::new(Semaphore::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(1));
    handle.try_tell(Record::new("first")).unwrap();

    let processed = Arc::new(Semaphore::new(0));
    let mut pending = Vec::with_capacity(WAITERS);
    for _ in 0..WAITERS {
        let handle = handle.clone();
        let processed = processed.clone();
        pending.push(tokio::spawn(async move {
            handle
                .tell(Record::with_processed_signal("pending", processed))
                .await
        }));
    }
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert!(
        pending.iter().all(|task| !task.is_finished()),
        "contended tells must wait while the bounded mailbox is full"
    );

    start_gate.add_permits(1);
    for task in pending {
        task.await.unwrap().unwrap();
    }
    processed
        .acquire_many(WAITERS as u32)
        .await
        .unwrap()
        .forget();
    assert_eq!(events.lock().await.len(), WAITERS + 1);
}
