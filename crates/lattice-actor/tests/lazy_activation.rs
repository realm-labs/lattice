use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use lattice_actor::{
    context::ActorContext,
    error::{ActorActivationError, ActorError},
    mailbox::MailboxConfig,
    registry::{ActorCreateContext, ActorLoader, ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, Responder},
};
use lattice_core::{actor_kind, id::ActorId};
use tokio::sync::Semaphore;

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, lattice_actor::Request)]
#[request(response = &'static str)]
struct Ping;

struct LazyActor;

#[async_trait]
impl Actor for LazyActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<Ping> for LazyActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: Ping,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send("pong");
        Ok(())
    }
}

#[derive(Clone)]
struct CountingLoader {
    loads: Arc<AtomicUsize>,
    release: Option<Arc<Semaphore>>,
    failures_remaining: Arc<AtomicUsize>,
}

#[async_trait]
impl ActorLoader<LazyActor> for CountingLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<LazyActor, ActorError> {
        assert_eq!(ctx.actor_kind, actor_kind!("Lazy"));
        assert_eq!(ctx.actor_id, ActorId::U64(7));
        self.loads.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = &self.release {
            release.acquire().await.unwrap().forget();
        }
        if self.failures_remaining.load(Ordering::SeqCst) > 0 {
            self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(ActorError::new("load failed"));
        }
        Ok(LazyActor)
    }
}

#[tokio::test]
async fn concurrent_lazy_activation_starts_one_local_actor() {
    let registry = Arc::new(ActorRegistry::<LazyActor>::new(
        actor_kind!("Lazy"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(8),
            ..ActorRegistryConfig::default()
        },
    ));
    let release = Arc::new(Semaphore::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let loader = CountingLoader {
        loads: loads.clone(),
        release: Some(release.clone()),
        failures_remaining: Arc::new(AtomicUsize::new(0)),
    };

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let registry = registry.clone();
        let loader = loader.clone();
        tasks.push(tokio::spawn(async move {
            registry.get_or_load(ActorId::U64(7), loader).await
        }));
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    release.add_permits(1);
    let first = tasks.pop().unwrap().await.unwrap().unwrap();
    assert_eq!(first.ask(Ping, ASK_TIMEOUT).await.unwrap(), "pong");
    for task in tasks {
        let handle = task.await.unwrap().unwrap();
        assert_eq!(handle.ask(Ping, ASK_TIMEOUT).await.unwrap(), "pong");
    }

    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn loader_failure_is_explicit_and_allows_retry() {
    let registry =
        ActorRegistry::<LazyActor>::new(actor_kind!("Lazy"), ActorRegistryConfig::default());
    let loads = Arc::new(AtomicUsize::new(0));
    let loader = CountingLoader {
        loads: loads.clone(),
        release: None,
        failures_remaining: Arc::new(AtomicUsize::new(1)),
    };

    let first = registry.get_or_load(ActorId::U64(7), loader.clone()).await;
    let second = registry.get_or_load(ActorId::U64(7), loader).await.unwrap();

    assert!(matches!(
        first,
        Err(ActorActivationError::ActivationFailed(_))
    ));
    assert_eq!(second.ask(Ping, ASK_TIMEOUT).await.unwrap(), "pong");
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}
