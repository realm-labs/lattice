//! Child ownership: a released child still stops once its system lane drains.

use std::{sync::Arc, time::Duration};

use tokio::sync::Semaphore;

use super::ASK_TIMEOUT;
use crate::{
    context::{ActorContext, HandlerContext},
    error::ActorError,
    handle::ActorHandle,
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::spawn_actor,
    traits::{Actor, ChildActorKey, ChildActorOptions, Handler, Responder},
};

#[tokio::test]
async fn stop_child_waits_for_system_lane_capacity() {
    struct ChildActor;

    impl Actor for ChildActor {
        type Error = ActorError;
        type Behavior = crate::state_machine::Stateless;
    }

    #[derive(Debug, crate::Message)]
    struct Park {
        entered: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    #[derive(Debug, crate::Message)]
    struct OccupySystemLane;

    #[derive(Debug, crate::Message)]
    struct ReleaseChild;

    #[derive(Debug, crate::Request)]
    #[request(response = ActorHandle<ChildActor>)]
    struct ChildHandle;

    impl Handler<Park> for ChildActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            msg: Park,
        ) -> Result<(), ActorError> {
            msg.entered.add_permits(1);
            msg.release.acquire().await.unwrap().forget();
            Ok(())
        }
    }

    impl Handler<OccupySystemLane> for ChildActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            _msg: OccupySystemLane,
        ) -> Result<(), ActorError> {
            Ok(())
        }
    }

    struct ParentActor {
        child: Option<ActorHandle<ChildActor>>,
    }

    impl Actor for ParentActor {
        type Error = ActorError;
        type Behavior = crate::state_machine::Stateless;

        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.child = Some(ctx.spawn_child(
                ChildActorKey::new("child"),
                ChildActor,
                ChildActorOptions {
                    mailbox: MailboxConfig::with_lanes(4, 1),
                    ..ChildActorOptions::default()
                },
            )?);
            Ok(())
        }
    }

    impl Responder<ChildHandle> for ParentActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            _request: ChildHandle,
            reply_to: ReplyTo<ActorHandle<ChildActor>>,
        ) -> Result<(), ActorError> {
            reply_to.send(self.child.clone().expect("child was spawned"))?;
            Ok(())
        }
    }

    impl Handler<ReleaseChild> for ParentActor {
        async fn handle(
            &mut self,
            ctx: &mut HandlerContext<'_, Self>,
            _msg: ReleaseChild,
        ) -> Result<(), ActorError> {
            assert!(ctx.stop_child(&ChildActorKey::new("child")));
            Ok(())
        }
    }

    let parent = spawn_actor(ParentActor { child: None }, MailboxConfig::bounded(8));
    let child = parent.ask(ChildHandle, ASK_TIMEOUT).await.unwrap();
    let mut terminated = child.subscribe_terminated();

    // Park the child, then occupy the only system slot so the stop request cannot be admitted.
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    child
        .try_tell_for_test(Park {
            entered: entered.clone(),
            release: release.clone(),
        })
        .unwrap();
    entered.acquire().await.unwrap().forget();
    child.try_tell_system_for_test(OccupySystemLane).unwrap();

    parent.tell(ReleaseChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    release.add_permits(1);

    tokio::time::timeout(Duration::from_secs(5), terminated.recv())
        .await
        .expect("released child stops once its system lane drains")
        .unwrap();
}
