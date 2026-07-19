//! Configurable Snowflake generators for process-wide and owner-local IDs.

use std::{
    cell::Cell,
    cmp::Ordering as CmpOrdering,
    sync::atomic::{AtomicU64, Ordering},
};

const UNINITIALIZED_STATE: u64 = u64::MAX;

pub const DEFAULT_EPOCH_MS: i64 = 1_767_225_600_000;
pub const DEFAULT_TIMESTAMP_BITS: u8 = 41;
pub const DEFAULT_WORKER_BITS: u8 = 10;
pub const DEFAULT_SEQUENCE_BITS: u8 = 12;
pub const DEFAULT_LOCAL_SEQUENCE_BITS: u8 = 22;

pub const DEFAULT_SNOWFLAKE_CONFIG: SnowflakeConfig = SnowflakeConfig {
    epoch_ms: DEFAULT_EPOCH_MS,
    timestamp_bits: DEFAULT_TIMESTAMP_BITS,
    worker_bits: DEFAULT_WORKER_BITS,
    sequence_bits: DEFAULT_SEQUENCE_BITS,
};

pub const DEFAULT_LOCAL_SNOWFLAKE_CONFIG: LocalSnowflakeConfig = LocalSnowflakeConfig {
    epoch_ms: DEFAULT_EPOCH_MS,
    timestamp_bits: DEFAULT_TIMESTAMP_BITS,
    sequence_bits: DEFAULT_LOCAL_SEQUENCE_BITS,
};

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum SnowflakeConfigError {
    #[error("snowflake timestamp bits must be greater than zero")]
    MissingTimestampBits,
    #[error("snowflake sequence bits must be greater than zero")]
    MissingSequenceBits,
    #[error("snowflake bit layout {timestamp_bits}/{worker_bits}/{sequence_bits} exceeds 63 bits")]
    TooManyBits {
        timestamp_bits: u8,
        worker_bits: u8,
        sequence_bits: u8,
    },
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum SnowflakeError {
    #[error("snowflake worker id {worker_id} exceeds {max_worker_id}")]
    InvalidWorkerId { worker_id: u64, max_worker_id: u64 },
    #[error("clock moved backwards from {last_ms} to {now_ms}")]
    ClockMovedBackwards { last_ms: i64, now_ms: i64 },
    #[error("timestamp {now_ms} is before snowflake epoch {epoch_ms}")]
    BeforeEpoch { now_ms: i64, epoch_ms: i64 },
    #[error("elapsed timestamp {elapsed_ms} exceeds configured maximum {max_elapsed_ms}")]
    TimestampOverflow {
        elapsed_ms: u64,
        max_elapsed_ms: u64,
    },
    #[error("snowflake sequence exhausted for millisecond {0}")]
    SequenceExhausted(i64),
    #[error("persisted local id {last_id} exceeds configured maximum {max_id}")]
    InvalidLastId { last_id: u64, max_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnowflakeConfig {
    epoch_ms: i64,
    timestamp_bits: u8,
    worker_bits: u8,
    sequence_bits: u8,
}

impl SnowflakeConfig {
    pub const fn new(
        epoch_ms: i64,
        timestamp_bits: u8,
        worker_bits: u8,
        sequence_bits: u8,
    ) -> Result<Self, SnowflakeConfigError> {
        if let Err(error) = validate_layout(timestamp_bits, worker_bits, sequence_bits) {
            return Err(error);
        }
        Ok(Self {
            epoch_ms,
            timestamp_bits,
            worker_bits,
            sequence_bits,
        })
    }

    pub const fn epoch_ms(self) -> i64 {
        self.epoch_ms
    }

    pub const fn timestamp_bits(self) -> u8 {
        self.timestamp_bits
    }

    pub const fn worker_bits(self) -> u8 {
        self.worker_bits
    }

    pub const fn sequence_bits(self) -> u8 {
        self.sequence_bits
    }

    pub const fn max_worker_id(self) -> u64 {
        bit_mask(self.worker_bits)
    }

    pub const fn max_sequence(self) -> u64 {
        bit_mask(self.sequence_bits)
    }

    pub const fn max_elapsed_ms(self) -> u64 {
        bit_mask(self.timestamp_bits)
    }
}

impl Default for SnowflakeConfig {
    fn default() -> Self {
        DEFAULT_SNOWFLAKE_CONFIG
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSnowflakeConfig {
    epoch_ms: i64,
    timestamp_bits: u8,
    sequence_bits: u8,
}

impl LocalSnowflakeConfig {
    pub const fn new(
        epoch_ms: i64,
        timestamp_bits: u8,
        sequence_bits: u8,
    ) -> Result<Self, SnowflakeConfigError> {
        if let Err(error) = validate_layout(timestamp_bits, 0, sequence_bits) {
            return Err(error);
        }
        Ok(Self {
            epoch_ms,
            timestamp_bits,
            sequence_bits,
        })
    }

    pub const fn epoch_ms(self) -> i64 {
        self.epoch_ms
    }

    pub const fn timestamp_bits(self) -> u8 {
        self.timestamp_bits
    }

    pub const fn sequence_bits(self) -> u8 {
        self.sequence_bits
    }

    pub const fn max_sequence(self) -> u64 {
        bit_mask(self.sequence_bits)
    }

    pub const fn max_elapsed_ms(self) -> u64 {
        bit_mask(self.timestamp_bits)
    }

    pub const fn max_id(self) -> u64 {
        (self.max_elapsed_ms() << self.sequence_bits) | self.max_sequence()
    }
}

impl Default for LocalSnowflakeConfig {
    fn default() -> Self {
        DEFAULT_LOCAL_SNOWFLAKE_CONFIG
    }
}

/// A lock-free Snowflake generator intended to be shared by a process.
///
/// Worker ID ownership remains an external lease and fencing concern.
#[derive(Debug)]
pub struct SnowflakeIdGenerator {
    config: SnowflakeConfig,
    worker_id: u64,
    state: SnowflakeState,
}

impl SnowflakeIdGenerator {
    pub fn new(worker_id: u64) -> Result<Self, SnowflakeError> {
        Self::with_config(worker_id, SnowflakeConfig::default())
    }

    pub fn with_config(worker_id: u64, config: SnowflakeConfig) -> Result<Self, SnowflakeError> {
        validate_worker(worker_id, config)?;
        Ok(Self {
            config,
            worker_id,
            state: SnowflakeState::new(),
        })
    }

    pub const fn config(&self) -> SnowflakeConfig {
        self.config
    }

    pub const fn worker_id(&self) -> u64 {
        self.worker_id
    }

    pub fn next_at(&self, now_ms: i64) -> Result<u64, SnowflakeError> {
        self.state.next_at(self.config, self.worker_id, now_ms)
    }
}

/// An owner-local generator with no worker segment.
///
/// `Cell` deliberately makes this type `Send` but not `Sync`.
#[derive(Debug)]
pub struct LocalIdGenerator {
    config: LocalSnowflakeConfig,
    state: Cell<u64>,
}

impl LocalIdGenerator {
    pub fn new() -> Self {
        Self::with_config(LocalSnowflakeConfig::default())
    }

    pub const fn with_config(config: LocalSnowflakeConfig) -> Self {
        Self {
            config,
            state: Cell::new(UNINITIALIZED_STATE),
        }
    }

    pub fn from_last_id(last_id: u64) -> Result<Self, SnowflakeError> {
        Self::with_config_and_last_id(LocalSnowflakeConfig::default(), last_id)
    }

    pub fn with_config_and_last_id(
        config: LocalSnowflakeConfig,
        last_id: u64,
    ) -> Result<Self, SnowflakeError> {
        if last_id > config.max_id() {
            return Err(SnowflakeError::InvalidLastId {
                last_id,
                max_id: config.max_id(),
            });
        }
        Ok(Self {
            config,
            state: Cell::new(last_id),
        })
    }

    pub const fn config(&self) -> LocalSnowflakeConfig {
        self.config
    }

    pub fn next_at(&self, now_ms: i64) -> Result<u64, SnowflakeError> {
        let next = next_state(
            self.config.epoch_ms,
            self.config.timestamp_bits,
            self.config.sequence_bits,
            self.state.get(),
            now_ms,
        )?;
        self.state.set(next.encoded);
        Ok(next.encoded)
    }

    pub fn last_id(&self) -> Option<u64> {
        let state = self.state.get();
        (state != UNINITIALIZED_STATE).then_some(state)
    }
}

impl Default for LocalIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct SnowflakeState {
    state: AtomicU64,
}

impl SnowflakeState {
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU64::new(UNINITIALIZED_STATE),
        }
    }

    pub(crate) fn next_at(
        &self,
        config: SnowflakeConfig,
        worker_id: u64,
        now_ms: i64,
    ) -> Result<u64, SnowflakeError> {
        validate_worker(worker_id, config)?;
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            let next = next_state(
                config.epoch_ms,
                config.timestamp_bits,
                config.sequence_bits,
                current,
                now_ms,
            )?;
            match self.state.compare_exchange_weak(
                current,
                next.encoded,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(
                        (next.elapsed_ms << (config.worker_bits + config.sequence_bits))
                            | (worker_id << config.sequence_bits)
                            | next.sequence,
                    );
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NextState {
    encoded: u64,
    elapsed_ms: u64,
    sequence: u64,
}

fn next_state(
    epoch_ms: i64,
    timestamp_bits: u8,
    sequence_bits: u8,
    current: u64,
    now_ms: i64,
) -> Result<NextState, SnowflakeError> {
    let elapsed_ms = elapsed_ms(now_ms, epoch_ms, bit_mask(timestamp_bits))?;
    let sequence = if current == UNINITIALIZED_STATE {
        0
    } else {
        let last_elapsed_ms = current >> sequence_bits;
        match elapsed_ms.cmp(&last_elapsed_ms) {
            CmpOrdering::Less => {
                return Err(SnowflakeError::ClockMovedBackwards {
                    last_ms: epoch_ms + last_elapsed_ms as i64,
                    now_ms,
                });
            }
            CmpOrdering::Equal => current
                .checked_add(1)
                .filter(|next| next & bit_mask(sequence_bits) != 0)
                .map(|next| next & bit_mask(sequence_bits))
                .ok_or(SnowflakeError::SequenceExhausted(now_ms))?,
            CmpOrdering::Greater => 0,
        }
    };
    Ok(NextState {
        encoded: (elapsed_ms << sequence_bits) | sequence,
        elapsed_ms,
        sequence,
    })
}

fn elapsed_ms(now_ms: i64, epoch_ms: i64, max: u64) -> Result<u64, SnowflakeError> {
    let elapsed = i128::from(now_ms) - i128::from(epoch_ms);
    if elapsed < 0 {
        return Err(SnowflakeError::BeforeEpoch { now_ms, epoch_ms });
    }
    if elapsed > i128::from(max) {
        return Err(SnowflakeError::TimestampOverflow {
            elapsed_ms: elapsed as u64,
            max_elapsed_ms: max,
        });
    }
    Ok(elapsed as u64)
}

fn validate_worker(worker_id: u64, config: SnowflakeConfig) -> Result<(), SnowflakeError> {
    if worker_id > config.max_worker_id() {
        return Err(SnowflakeError::InvalidWorkerId {
            worker_id,
            max_worker_id: config.max_worker_id(),
        });
    }
    Ok(())
}

const fn validate_layout(
    timestamp_bits: u8,
    worker_bits: u8,
    sequence_bits: u8,
) -> Result<(), SnowflakeConfigError> {
    if timestamp_bits == 0 {
        return Err(SnowflakeConfigError::MissingTimestampBits);
    }
    if sequence_bits == 0 {
        return Err(SnowflakeConfigError::MissingSequenceBits);
    }
    if timestamp_bits as u16 + worker_bits as u16 + sequence_bits as u16 > 63 {
        return Err(SnowflakeConfigError::TooManyBits {
            timestamp_bits,
            worker_bits,
            sequence_bits,
        });
    }
    Ok(())
}

const fn bit_mask(bits: u8) -> u64 {
    (1_u64 << bits) - 1
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, thread::spawn as spawn_thread};

    use super::{
        DEFAULT_EPOCH_MS, LocalIdGenerator, LocalSnowflakeConfig, SnowflakeConfig,
        SnowflakeConfigError, SnowflakeError, SnowflakeIdGenerator,
    };

    #[test]
    fn default_config_preserves_the_project_layout() {
        let config = SnowflakeConfig::default();
        assert_eq!(config.epoch_ms(), DEFAULT_EPOCH_MS);
        assert_eq!(config.timestamp_bits(), 41);
        assert_eq!(config.worker_bits(), 10);
        assert_eq!(config.sequence_bits(), 12);
        assert_eq!(config.max_worker_id(), 1023);
        assert_eq!(config.max_sequence(), 4095);
    }

    #[test]
    fn rejects_invalid_layouts() {
        assert_eq!(
            SnowflakeConfig::new(0, 0, 10, 12),
            Err(SnowflakeConfigError::MissingTimestampBits)
        );
        assert_eq!(
            SnowflakeConfig::new(0, 41, 10, 0),
            Err(SnowflakeConfigError::MissingSequenceBits)
        );
        assert!(matches!(
            SnowflakeConfig::new(0, 42, 10, 12),
            Err(SnowflakeConfigError::TooManyBits { .. })
        ));
    }

    #[test]
    fn custom_config_controls_all_id_segments() {
        let config = SnowflakeConfig::new(1_000, 8, 3, 4).unwrap();
        let generator = SnowflakeIdGenerator::with_config(5, config).unwrap();
        assert_eq!(generator.next_at(1_002), Ok((2 << 7) | (5 << 4)));
        assert_eq!(generator.next_at(1_002), Ok((2 << 7) | (5 << 4) | 1));
    }

    #[test]
    fn validates_worker_and_timestamp_ranges() {
        let config = SnowflakeConfig::new(1_000, 8, 3, 4).unwrap();
        assert!(matches!(
            SnowflakeIdGenerator::with_config(8, config),
            Err(SnowflakeError::InvalidWorkerId { .. })
        ));
        let generator = SnowflakeIdGenerator::with_config(7, config).unwrap();
        assert!(matches!(
            generator.next_at(999),
            Err(SnowflakeError::BeforeEpoch { .. })
        ));
        assert!(matches!(
            generator.next_at(1_256),
            Err(SnowflakeError::TimestampOverflow { .. })
        ));
    }

    #[test]
    fn distributed_generator_is_unique_under_contention() {
        let generator = Arc::new(SnowflakeIdGenerator::new(42).unwrap());
        let ids = (0..8)
            .map(|_| {
                let generator = generator.clone();
                spawn_thread(move || {
                    (0..256)
                        .map(|_| generator.next_at(DEFAULT_EPOCH_MS + 1).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 8 * 256);
    }

    #[test]
    fn local_generator_uses_timestamp_and_sequence_without_worker() {
        fn assert_send<T: Send>() {}
        assert_send::<LocalIdGenerator>();
        let config = LocalSnowflakeConfig::new(1_000, 8, 7).unwrap();
        let generator = LocalIdGenerator::with_config(config);
        assert_eq!(generator.next_at(1_002), Ok(2 << 7));
        assert_eq!(generator.next_at(1_002), Ok((2 << 7) | 1));
        assert_eq!(generator.next_at(1_003), Ok(3 << 7));
    }

    #[test]
    fn local_generator_can_resume_and_reports_unsafe_time() {
        let config = LocalSnowflakeConfig::new(1_000, 8, 2).unwrap();
        let last_id = (2 << 2) | 2;
        let generator = LocalIdGenerator::with_config_and_last_id(config, last_id).unwrap();
        assert_eq!(generator.last_id(), Some(last_id));
        assert_eq!(generator.next_at(1_002), Ok(last_id + 1));
        assert_eq!(
            generator.next_at(1_002),
            Err(SnowflakeError::SequenceExhausted(1_002))
        );
        assert!(matches!(
            generator.next_at(1_001),
            Err(SnowflakeError::ClockMovedBackwards { .. })
        ));
    }
}
