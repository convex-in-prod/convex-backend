use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

use bytes::Bytes;
use tokio::sync::Notify;

pub static PACKED_BUFFER_MEMORY: BufferMemory = BufferMemory::new();

/// Live backing bytes, including allocations pinned after cache eviction.
/// Cache weights and document HeapSize remain logical sizes; neither owns this
/// charge.
pub struct BufferMemory {
    bytes: AtomicUsize,
    changed: Notify,
}

impl Default for BufferMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferMemory {
    pub const fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
            changed: Notify::const_new(),
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Acquire)
    }

    pub fn changed(&self) -> &Notify {
        &self.changed
    }

    /// Take a fresh allocation before any references or slices can escape.
    /// Accepting arbitrary Bytes here would allow wrapping one allocation
    /// twice.
    pub fn track(&'static self, data: Vec<u8>) -> Bytes {
        // Tighten before freezing: Bytes::from_owner cannot recover a Vec without
        // copying, and system documents previously shrank their backing afterward.
        let data = data.into_boxed_slice();
        let bytes = data.len();
        if bytes == 0 {
            return Bytes::new();
        }
        self.bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes)
            })
            .expect("packed buffer accounting overflow");
        Bytes::from_owner(BufferOwner {
            data,
            _charge: BufferCharge {
                memory: self,
                bytes,
            },
        })
    }
}

struct BufferOwner {
    // Field drop order matters: free the allocation before releasing its charge.
    data: Box<[u8]>,
    _charge: BufferCharge,
}

impl AsRef<[u8]> for BufferOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

struct BufferCharge {
    memory: &'static BufferMemory,
    bytes: usize,
}

impl Drop for BufferCharge {
    fn drop(&mut self) {
        self.memory
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(self.bytes)
            })
            .expect("packed buffer accounting underflow");
        // The last reader can be a database/cache owner, with no Wasm invocation
        // finishing to wake admission. Use the same notification as the controller.
        self.memory.changed.notify_waiters();
    }
}
