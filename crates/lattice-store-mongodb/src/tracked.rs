//! Conservative actor-local mutation tracking.

/// Wraps an ordinary value and advances an epoch whenever mutable access is
/// requested. The epoch is a scan candidate hint, not proof that the value
/// changed; acknowledged scan baselines remain the correctness boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tracked<T> {
    value: T,
    mutation_epoch: u64,
}

impl<T> Tracked<T> {
    pub const fn clean(value: T) -> Self {
        Self {
            value,
            mutation_epoch: 0,
        }
    }

    pub const fn read(&self) -> &T {
        &self.value
    }

    pub fn write(&mut self) -> &mut T {
        self.mutation_epoch = self
            .mutation_epoch
            .checked_add(1)
            .expect("tracked mutation epoch exhausted");
        &mut self.value
    }

    pub const fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }
}

impl<T> std::ops::Deref for Tracked<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.read()
    }
}

impl<T> std::ops::DerefMut for Tracked<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.write()
    }
}

#[cfg(test)]
mod tests {
    use super::Tracked;

    #[test]
    fn mutable_access_advances_epoch_even_when_value_is_unchanged() {
        let mut value = Tracked::clean(vec![1]);
        assert_eq!(value.mutation_epoch(), 0);
        let _ = value.write();
        assert_eq!(value.mutation_epoch(), 1);
        value.push(2);
        assert_eq!(value.mutation_epoch(), 2);
    }
}
