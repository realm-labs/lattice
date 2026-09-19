//! Immutable runtime integration data attached to one Actor activation.

use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt,
    sync::Arc,
};

use thiserror::Error;

/// Type-indexed attachments installed by a runtime integration for one Actor activation.
///
/// Attachments let integrations expose activation-specific metadata without adding their concepts
/// to [`crate::context::ActorContext`]. The attachment map is frozen before the Actor starts and is
/// not inherited by child Actors. Mutable business state belongs on the Actor itself.
#[derive(Clone, Default)]
pub struct ActorRuntimeAttachments {
    values: Arc<HashMap<TypeId, StoredAttachment>>,
}

impl ActorRuntimeAttachments {
    pub fn empty() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn builder() -> ActorRuntimeAttachmentsBuilder {
        ActorRuntimeAttachmentsBuilder::default()
    }

    #[doc(hidden)]
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|attachment| attachment.value.clone().downcast::<T>().ok())
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for ActorRuntimeAttachments {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRuntimeAttachments")
            .field("count", &self.values.len())
            .finish()
    }
}

#[doc(hidden)]
#[derive(Default)]
pub struct ActorRuntimeAttachmentsBuilder {
    values: HashMap<TypeId, StoredAttachment>,
}

impl ActorRuntimeAttachmentsBuilder {
    pub fn insert<T>(&mut self, value: T) -> Result<&mut Self, ActorRuntimeAttachmentError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.values.contains_key(&type_id) {
            return Err(ActorRuntimeAttachmentError::Duplicate {
                type_name: type_name::<T>(),
            });
        }
        self.values.insert(
            type_id,
            StoredAttachment {
                value: Arc::new(value),
            },
        );
        Ok(self)
    }

    pub fn build(self) -> ActorRuntimeAttachments {
        ActorRuntimeAttachments {
            values: Arc::new(self.values),
        }
    }
}

struct StoredAttachment {
    value: Arc<dyn Any + Send + Sync>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorRuntimeAttachmentError {
    #[error("Actor runtime attachment {type_name} is already installed")]
    Duplicate { type_name: &'static str },
}
