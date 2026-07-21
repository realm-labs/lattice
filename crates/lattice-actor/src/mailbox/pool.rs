use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::ptr::NonNull;
use std::sync::OnceLock;

use concurrent_queue::ConcurrentQueue;

const BLOCK_SIZES: [usize; 5] = [64, 128, 256, 512, 1024];
const BLOCK_ALIGNMENT: usize = 64;

struct FreeBlock(NonNull<u8>);

// SAFETY: a free block contains no initialized value, and ownership is transferred into or out of
// the concurrent queue. Only the thread that successfully pops it may initialize or deallocate it.
unsafe impl Send for FreeBlock {}

pub(super) struct BlockPool {
    classes: [OnceLock<ConcurrentQueue<FreeBlock>>; BLOCK_SIZES.len()],
    retained_per_class: usize,
}

impl BlockPool {
    pub(super) const fn new(retained_per_class: usize) -> Self {
        assert!(retained_per_class > 0);
        Self {
            classes: [const { OnceLock::new() }; BLOCK_SIZES.len()],
            retained_per_class,
        }
    }

    pub(super) fn allocate(&self, layout: Layout) -> NonNull<u8> {
        if let Some(class) = class_for(layout) {
            if let Ok(FreeBlock(pointer)) = self.class(class).pop() {
                return pointer;
            }
            return allocate_uncached(layout);
        }
        allocate_uncached(layout)
    }

    pub(super) fn recycle(&self, pointer: NonNull<u8>, layout: Layout) {
        if let Some(class) = class_for(layout) {
            let Err(error) = self.class(class).push(FreeBlock(pointer)) else {
                return;
            };
            let FreeBlock(pointer) = error.into_inner();
            // SAFETY: the block was allocated with this class layout and is no longer initialized.
            deallocate_uncached(pointer, layout);
            return;
        }
        deallocate_uncached(pointer, layout);
    }

    fn class(&self, class: usize) -> &ConcurrentQueue<FreeBlock> {
        self.classes[class].get_or_init(|| ConcurrentQueue::bounded(self.retained_per_class))
    }
}

pub(super) struct LocalBlockPool {
    classes: [Vec<FreeBlock>; BLOCK_SIZES.len()],
    retained_per_class: usize,
}

impl LocalBlockPool {
    pub(super) const fn new(retained_per_class: usize) -> Self {
        assert!(retained_per_class > 0);
        Self {
            classes: [const { Vec::new() }; BLOCK_SIZES.len()],
            retained_per_class,
        }
    }

    pub(super) fn allocate(&mut self, layout: Layout) -> NonNull<u8> {
        if let Some(class) = class_for(layout)
            && let Some(FreeBlock(pointer)) = self.classes[class].pop()
        {
            return pointer;
        }
        allocate_uncached(layout)
    }

    pub(super) fn recycle(&mut self, pointer: NonNull<u8>, layout: Layout) {
        if let Some(class) = class_for(layout)
            && self.classes[class].len() < self.retained_per_class
        {
            self.classes[class].push(FreeBlock(pointer));
            return;
        }
        deallocate_uncached(pointer, layout);
    }
}

impl Drop for LocalBlockPool {
    fn drop(&mut self) {
        for (class, blocks) in self.classes.iter_mut().enumerate() {
            for FreeBlock(pointer) in blocks.drain(..) {
                // SAFETY: cached blocks in this vector were allocated with this class layout.
                unsafe { dealloc(pointer.as_ptr(), class_layout(class)) };
            }
        }
    }
}

fn class_for(layout: Layout) -> Option<usize> {
    if layout.align() > BLOCK_ALIGNMENT {
        return None;
    }
    BLOCK_SIZES.iter().position(|size| layout.size() <= *size)
}

fn class_layout(class: usize) -> Layout {
    Layout::from_size_align(BLOCK_SIZES[class], BLOCK_ALIGNMENT)
        .expect("pooled actor allocation class has a valid layout")
}

pub(super) fn allocate_uncached(layout: Layout) -> NonNull<u8> {
    let allocation_layout = class_for(layout).map_or(layout, class_layout);
    // SAFETY: `allocation_layout` is valid and the caller takes ownership of the storage.
    let pointer = unsafe { alloc(allocation_layout) };
    NonNull::new(pointer).unwrap_or_else(|| handle_alloc_error(allocation_layout))
}

pub(super) fn deallocate_uncached(pointer: NonNull<u8>, layout: Layout) {
    let allocation_layout = class_for(layout).map_or(layout, class_layout);
    // SAFETY: the pointer came from `allocate_uncached` for this concrete layout or its class.
    unsafe { dealloc(pointer.as_ptr(), allocation_layout) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_layouts_are_classed_and_oversized_layouts_fall_back() {
        assert_eq!(class_for(Layout::from_size_align(1, 1).unwrap()), Some(0));
        assert_eq!(class_for(Layout::from_size_align(129, 8).unwrap()), Some(2));
        assert_eq!(class_for(Layout::from_size_align(2048, 8).unwrap()), None);
        assert_eq!(class_for(Layout::from_size_align(128, 128).unwrap()), None);
    }
}
