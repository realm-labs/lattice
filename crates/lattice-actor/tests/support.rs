use std::sync::OnceLock;

use lattice_actor::{
    handle::ActorHandle,
    mailbox::MailboxConfig,
    runtime::{ActorRuntime, ActorSpawnOptions},
    traits::Actor,
};

pub fn spawn_actor<A>(actor: A, mailbox: MailboxConfig) -> ActorHandle<A>
where
    A: Actor,
{
    static RUNTIME: OnceLock<ActorRuntime> = OnceLock::new();
    RUNTIME
        .get_or_init(ActorRuntime::default)
        .spawn_actor(
            actor,
            ActorSpawnOptions {
                mailbox,
                ..ActorSpawnOptions::default()
            },
        )
        .expect("test Actor should spawn")
}
