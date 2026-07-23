use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        record_deallocation(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            adjust_live_bytes(layout.size(), new_size);
        }
        new_pointer
    }
}

fn record_allocation(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn record_deallocation(size: usize) {
    DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    DEALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    LIVE_BYTES.fetch_sub(size, Ordering::Relaxed);
}

fn adjust_live_bytes(old_size: usize, new_size: usize) {
    if new_size >= old_size {
        let growth = new_size - old_size;
        let live = LIVE_BYTES.fetch_add(growth, Ordering::Relaxed) + growth;
        PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
    } else {
        LIVE_BYTES.fetch_sub(old_size - new_size, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AllocationSnapshot {
    pub allocations: u64,
    pub deallocations: u64,
    pub reallocations: u64,
    pub allocated_bytes: u64,
    pub deallocated_bytes: u64,
    pub live_bytes: usize,
    pub peak_live_bytes: usize,
}

impl AllocationSnapshot {
    pub fn now() -> Self {
        Self {
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
            reallocations: REALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
            live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
            peak_live_bytes: PEAK_LIVE_BYTES.load(Ordering::Relaxed),
        }
    }

    pub fn delta_since(self, earlier: Self) -> AllocationDelta {
        AllocationDelta {
            allocations: self.allocations.saturating_sub(earlier.allocations),
            deallocations: self.deallocations.saturating_sub(earlier.deallocations),
            reallocations: self.reallocations.saturating_sub(earlier.reallocations),
            allocated_bytes: self.allocated_bytes.saturating_sub(earlier.allocated_bytes),
            deallocated_bytes: self
                .deallocated_bytes
                .saturating_sub(earlier.deallocated_bytes),
            live_bytes_change: self.live_bytes as i128 - earlier.live_bytes as i128,
            peak_live_bytes: self.peak_live_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AllocationDelta {
    pub allocations: u64,
    pub deallocations: u64,
    pub reallocations: u64,
    pub allocated_bytes: u64,
    pub deallocated_bytes: u64,
    pub live_bytes_change: i128,
    pub peak_live_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessCpuTime {
    pub user: Duration,
    pub system: Duration,
}

impl ProcessCpuTime {
    pub fn total(self) -> Duration {
        self.user.saturating_add(self.system)
    }

    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            user: self.user.saturating_sub(earlier.user),
            system: self.system.saturating_sub(earlier.system),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResourceSnapshot {
    pub allocations: AllocationSnapshot,
    pub cpu: Option<ProcessCpuTime>,
}

impl ResourceSnapshot {
    pub fn now() -> Self {
        Self {
            allocations: AllocationSnapshot::now(),
            cpu: process_cpu_time(),
        }
    }

    pub fn delta_since(self, earlier: Self) -> ResourceDelta {
        ResourceDelta {
            allocations: self.allocations.delta_since(earlier.allocations),
            cpu: self
                .cpu
                .zip(earlier.cpu)
                .map(|(current, previous)| current.delta_since(previous)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResourceDelta {
    pub allocations: AllocationDelta,
    pub cpu: Option<ProcessCpuTime>,
}

#[cfg(unix)]
fn process_cpu_time() -> Option<ProcessCpuTime> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the pointed-to rusage on success. The pointer
    // is valid for writes and is not read unless the call reports success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful getrusage call above initialized the value.
    let usage = unsafe { usage.assume_init() };
    Some(ProcessCpuTime {
        user: timeval_duration(usage.ru_utime),
        system: timeval_duration(usage.ru_stime),
    })
}

#[cfg(unix)]
fn timeval_duration(value: libc::timeval) -> Duration {
    let seconds = u64::try_from(value.tv_sec).unwrap_or(0);
    let micros = u32::try_from(value.tv_usec).unwrap_or(0);
    Duration::new(seconds, micros.saturating_mul(1_000))
}

#[cfg(not(unix))]
fn process_cpu_time() -> Option<ProcessCpuTime> {
    None
}

#[cfg(test)]
mod tests {
    use super::{AllocationSnapshot, CountingAllocator, ProcessCpuTime};
    use std::time::Duration;

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    #[test]
    fn allocation_delta_is_monotonic() {
        let before = AllocationSnapshot::now();
        let allocation = vec![0_u8; 4_096];
        let after = AllocationSnapshot::now();
        assert!(after.allocated_bytes >= before.allocated_bytes + allocation.len() as u64);
    }

    #[test]
    fn cpu_delta_saturates() {
        let earlier = ProcessCpuTime {
            user: Duration::from_secs(2),
            system: Duration::from_secs(1),
        };
        let current = ProcessCpuTime {
            user: Duration::from_secs(1),
            system: Duration::from_secs(3),
        };
        let delta = current.delta_since(earlier);
        assert_eq!(delta.user, Duration::ZERO);
        assert_eq!(delta.system, Duration::from_secs(2));
    }
}
