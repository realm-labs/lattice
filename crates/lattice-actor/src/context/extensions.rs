//! Type-indexed storage owned by a single actor activation.

use std::{any::TypeId, fmt};

use super::ActorLocalExtensions;

impl fmt::Debug for ActorLocalExtensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorLocalExtensions")
            .field("extension_count", &self.values.len())
            .finish()
    }
}

impl ActorLocalExtensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<T: Send + 'static>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    pub fn get_mut<T: Send + 'static>(&mut self) -> Option<&mut T> {
        self.values
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut::<T>())
    }

    pub fn insert<T: Send + 'static>(&mut self, value: T) -> Option<T> {
        self.values
            .insert(TypeId::of::<T>(), Box::new(value))
            .map(|previous| {
                *previous
                    .downcast::<T>()
                    .expect("actor-local extension type ID invariant violated")
            })
    }

    pub fn remove<T: Send + 'static>(&mut self) -> Option<T> {
        self.values.remove(&TypeId::of::<T>()).map(|value| {
            *value
                .downcast::<T>()
                .expect("actor-local extension type ID invariant violated")
        })
    }

    pub fn get_or_insert_with<T: Send + 'static>(&mut self, create: impl FnOnce() -> T) -> &mut T {
        self.values
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(create()))
            .downcast_mut::<T>()
            .expect("actor-local extension type ID invariant violated")
    }
}
