use std::any::type_name;

use tracing::warn;

use super::{ActorEnvelope, EnvelopeFuture};
use crate::{
    context::{ActorContext, HandlerContext},
    traits::{Actor, MessageKind, MessageLane, MessageMetadata, MessageOutcome, MessageView},
};

pub(crate) struct ContinuationEnvelope<Output, Continue> {
    output: Option<Output>,
    continuation: Option<Continue>,
}

impl<Output, Continue> ContinuationEnvelope<Output, Continue> {
    pub(crate) fn new(output: Output, continuation: Continue) -> Self {
        Self {
            output: Some(output),
            continuation: Some(continuation),
        }
    }
}

impl<A, Output, Continue> ActorEnvelope<A> for ContinuationEnvelope<Output, Continue>
where
    A: Actor,
    Output: Send + 'static,
    Continue:
        FnOnce(&mut A, &mut HandlerContext<'_, A>, Output) -> Result<(), A::Error> + Send + 'static,
{
    fn metadata(&self, lane: super::MailboxLane) -> MessageMetadata {
        MessageMetadata::new(
            type_name::<Output>(),
            MessageKind::Continuation,
            MessageLane::from(lane),
            None,
        )
    }

    fn handle<'a>(
        &'a mut self,
        actor: &'a mut A,
        behavior: &'a mut A::Behavior,
        context: &'a mut ActorContext<A>,
        metadata: &'a MessageMetadata,
    ) -> EnvelopeFuture<'a> {
        EnvelopeFuture::new(async move {
            context.clear_sender();
            context.set_current_deadline(None);
            let output = self
                .output
                .as_ref()
                .expect("continuation output is present before dispatch");
            actor.before_message(context, MessageView::new(metadata, output));

            let output = self
                .output
                .take()
                .expect("continuation output is present before dispatch");
            let continuation = self
                .continuation
                .take()
                .expect("continuation callback is present before dispatch");
            let outcome =
                match continuation(actor, &mut HandlerContext::new(context, behavior), output) {
                    Ok(()) => MessageOutcome::Handled,
                    Err(error) => {
                        warn!(
                            continuation.output = type_name::<Output>(),
                            %error,
                            "actor continuation returned error"
                        );
                        actor.on_error::<Continue>(context, metadata, &error).await;
                        MessageOutcome::HandlerFailed
                    }
                };
            actor.after_message(context, metadata, outcome);
            context.clear_sender();
            context.set_current_deadline(None);
            outcome
        })
    }
}
