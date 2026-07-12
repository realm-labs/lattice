#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scheduled(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimClock {
    now_millis: u64,
}

impl SimClock {
    pub fn new() -> Self {
        Self { now_millis: 0 }
    }

    pub fn now_millis(&self) -> u64 {
        self.now_millis
    }

    pub fn advance_to(&mut self, deadline: u64) {
        self.now_millis = self.now_millis.max(deadline);
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SimScheduler<E> {
    seed: u64,
    next_id: u64,
    pending: Vec<(u64, u64, E)>,
}

impl<E> SimScheduler<E> {
    pub fn new(seed: u64) -> Self {
        Self {
            seed: seed.max(1),
            next_id: 1,
            pending: Vec::new(),
        }
    }

    pub fn schedule(&mut self, at_millis: u64, event: E) -> Scheduled {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.pending.push((at_millis, id, event));
        Scheduled(id)
    }

    pub fn pop_next(&mut self) -> Option<(u64, E)> {
        let minimum = self.pending.iter().map(|item| item.0).min()?;
        let ready = self.pending.iter().filter(|item| item.0 == minimum).count();
        let selected = (self.next_random() as usize) % ready;
        let index = self
            .pending
            .iter()
            .enumerate()
            .filter(|(_, item)| item.0 == minimum)
            .nth(selected)
            .map(|(index, _)| index)
            .expect("selected scheduled event");
        let (_, _, event) = self.pending.swap_remove(index);
        Some((minimum, event))
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn next_random(&mut self) -> u64 {
        let mut value = self.seed;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.seed = value;
        value
    }
}
