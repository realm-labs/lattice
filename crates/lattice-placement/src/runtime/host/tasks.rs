use std::{collections::HashMap, future::Future};

use tokio::task::{Id, JoinError, JoinSet};

/// Keeps scope ownership outside task futures so panic and cancellation cannot lose it.
pub(super) struct OwnedTasks<K, T> {
    tasks: JoinSet<T>,
    owners: HashMap<Id, K>,
}

impl<K, T: 'static> OwnedTasks<K, T> {
    pub(super) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            owners: HashMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(super) fn spawn(&mut self, owner: K, future: impl Future<Output = T> + Send + 'static)
    where
        T: Send,
    {
        let task = self.tasks.spawn(future);
        self.owners.insert(task.id(), owner);
    }

    pub(super) async fn join_next(&mut self) -> Option<(K, Result<T, JoinError>)> {
        let (id, outcome) = match self.tasks.join_next_with_id().await? {
            Ok((id, result)) => (id, Ok(result)),
            Err(error) => (error.id(), Err(error)),
        };
        let owner = self
            .owners
            .remove(&id)
            .expect("host task has a scope owner");
        Some((owner, outcome))
    }

    pub(super) fn abort_all(&mut self) {
        self.tasks.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::OwnedTasks;

    #[tokio::test]
    async fn ownership_survives_task_panic_and_cancellation() {
        let mut tasks = OwnedTasks::new();
        tasks.spawn("panicking-domain", async {
            panic!("injected task failure")
        });
        let (owner, outcome) = tasks.join_next().await.unwrap();
        assert_eq!(owner, "panicking-domain");
        assert!(outcome.unwrap_err().is_panic());
        tasks.spawn("cancelled-election", std::future::pending::<()>());
        tasks.abort_all();
        let (owner, outcome) = tasks.join_next().await.unwrap();
        assert_eq!(owner, "cancelled-election");
        assert!(outcome.unwrap_err().is_cancelled());
        assert!(tasks.is_empty());
        assert!(tasks.owners.is_empty());
    }
}
