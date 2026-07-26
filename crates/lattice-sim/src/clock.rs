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
pub struct SimRandom {
    state: u64,
}

impl SimRandom {
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    pub fn below(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            0
        } else {
            usize::try_from(self.next_u64() % bound as u64).unwrap_or(0)
        }
    }

    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        denominator != 0 && self.next_u64() % denominator < numerator
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            items.swap(index, self.below(index + 1));
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimScheduler<E> {
    random: SimRandom,
    next_id: u64,
    pending: Vec<(u64, u64, E)>,
}

impl<E> SimScheduler<E> {
    pub fn new(seed: u64) -> Self {
        Self {
            random: SimRandom::new(seed),
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
        let selected = self.random.below(ready);
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
}
