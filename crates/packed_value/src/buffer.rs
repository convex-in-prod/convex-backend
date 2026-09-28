use std::{
    fmt,
    ops::{
        Deref,
        Range,
    },
    str::{
        self,
        Utf8Error,
    },
};

use bytes::Bytes;
use flexbuffers::Buffer;
use scoped_tls::scoped_thread_local;
use serde::{
    de::{
        Error,
        Visitor,
    },
    Deserialize,
    Serialize,
    Serializer,
};
use value::heap_size::HeapSize;

use crate::memory::PACKED_BUFFER_MEMORY;

scoped_thread_local!(pub(crate) static PARENT_BUFFER: Bytes);

#[derive(Clone, Debug, Eq)]
pub struct ByteBuffer {
    pub(crate) inner: Bytes,
    backing_len: usize,
}

impl PartialEq for ByteBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl From<Vec<u8>> for ByteBuffer {
    fn from(buf: Vec<u8>) -> Self {
        Self {
            backing_len: buf.len(),
            inner: PACKED_BUFFER_MEMORY.track(buf),
        }
    }
}

impl ByteBuffer {
    pub fn shrink(self) -> Self {
        if self.inner.len() == self.backing_len {
            self
        } else {
            // Only a slice needs detaching. Whole buffers were tightened before
            // tracking; copying them here would defeat zero-copy cache sharing.
            self.inner.to_vec().into()
        }
    }
}

impl Deref for ByteBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.inner.deref()
    }
}

impl Buffer for ByteBuffer {
    type BufferString = StringBuffer;

    fn slice(&self, range: Range<usize>) -> Option<Self> {
        if range.start > range.end || range.end >= self.inner.len() {
            return None;
        }
        Some(Self {
            inner: self.inner.slice(range),
            backing_len: self.backing_len,
        })
    }

    fn empty() -> Self {
        Self {
            inner: Bytes::new(),
            backing_len: 0,
        }
    }

    fn buffer_str(&self) -> Result<Self::BufferString, Utf8Error> {
        str::from_utf8(&self.inner[..])?;
        Ok(StringBuffer {
            inner: self.inner.clone(),
        })
    }

    fn shallow_copy(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            backing_len: self.backing_len,
        }
    }

    fn empty_str() -> Self::BufferString {
        StringBuffer {
            inner: Bytes::new(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringBuffer {
    inner: Bytes,
}

impl fmt::Debug for StringBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl Deref for StringBuffer {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.inner[..]) }
    }
}

impl Serialize for StringBuffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StringBuffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visit;
        impl<'de> Visitor<'de> for Visit {
            type Value = StringBuffer;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // If `v` is a subslice of `PARENT_BUFFER` (i.e. the buffer that
                // is currently being parsed with
                // `PackedValue<ByteBuffer>::parse`), then return a subslice of
                // that buffer instead of copying it
                if PARENT_BUFFER.is_set()
                    && let Some(v) = PARENT_BUFFER.with(|parent| {
                        if let Some(range) = parent.subslice_range(v.as_bytes()) {
                            Some(parent.slice(range))
                        } else {
                            None
                        }
                    })
                {
                    return Ok(StringBuffer { inner: v });
                }
                Ok(StringBuffer {
                    inner: PACKED_BUFFER_MEMORY.track(v.as_bytes().to_owned()),
                })
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(StringBuffer::new(v))
            }
        }
        deserializer.deserialize_str(Visit)
    }
}

impl StringBuffer {
    pub fn new(s: String) -> StringBuffer {
        StringBuffer {
            inner: PACKED_BUFFER_MEMORY.track(s.into_bytes()),
        }
    }
}

impl HeapSize for StringBuffer {
    fn heap_size(&self) -> usize {
        self.inner.heap_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::BufferMemory;

    #[test]
    fn backing_charge_survives_cache_eviction_and_all_slice_paths() {
        static MEMORY: BufferMemory = BufferMemory::new();
        let mut data = Vec::with_capacity(1024);
        data.extend_from_slice(b"abcdef");
        let cached = ByteBuffer {
            backing_len: data.len(),
            inner: MEMORY.track(data),
        };
        assert_eq!(MEMORY.bytes(), 6);
        let reader = cached.clone();
        let shallow = cached.shallow_copy();
        let slice = cached.slice(1..4).unwrap();
        let string = slice.buffer_str().unwrap();
        let parsed = PARENT_BUFFER.set(&cached.inner, || {
            StringBuffer::deserialize(
                serde::de::value::StrDeserializer::<serde::de::value::Error>::new(
                    &reader.buffer_str().unwrap()[2..5],
                ),
            )
            .unwrap()
        });
        drop(cached);
        drop(reader);
        drop(shallow);
        drop(slice);
        assert_eq!(MEMORY.bytes(), 6);
        assert_eq!(&*string, "bcd");
        assert_eq!(&*parsed, "cde");
        drop(string);
        assert_eq!(MEMORY.bytes(), 6);
        std::thread::spawn(move || drop(parsed)).join().unwrap();
        assert_eq!(MEMORY.bytes(), 0);
    }

    #[test]
    fn shrink_reuses_whole_backing_and_detaches_slices() {
        let whole = ByteBuffer::from(b"abcdef".to_vec());
        let pointer = whole.as_ptr();
        let slice = whole.slice(1..4).unwrap();
        let whole = whole.shrink();
        assert_eq!(whole.as_ptr(), pointer);
        let compact = slice.clone().shrink();
        assert_eq!(compact, slice);
        assert_eq!(compact.backing_len, 3);
        assert_ne!(compact.as_ptr(), slice.as_ptr());
    }
}
