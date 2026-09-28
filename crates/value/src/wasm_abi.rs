//! Bounded value encoding for the Static Hermes guest boundary.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    array::MAX_ARRAY_LEN,
    ConvexBytes,
    ConvexString,
    ConvexValue,
    FieldName,
    PendingValue,
    MAX_NESTING,
    MAX_OBJECT_FIELDS,
};

pub const VALUE_ABI_MAGIC: &[u8; 4] = b"CVA1";
pub const PACKED_DOCUMENT_TAG: u8 = 10;
pub const DOCUMENT_COLLECTION_TAG: u8 = 11;
pub const PATCH_DELETE_TAG: u8 = 12;

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const FLOAT64: u8 = 3;
const INT64: u8 = 4;
const STRING: u8 = 5;
const BYTES: u8 = 6;
const ARRAY: u8 = 7;
const OBJECT: u8 = 8;
const COMMIT_TS: u8 = 9;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValueAbiError {
    #[error("value ABI payload exceeds the configured byte limit")]
    TooLarge,
    #[error("value ABI payload is malformed")]
    Malformed,
    #[error("value ABI payload contains an invalid Convex value")]
    InvalidValue,
}

struct Writer {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl Writer {
    fn new(maximum_bytes: usize) -> Result<Self, ValueAbiError> {
        let mut writer = Self {
            bytes: Vec::new(),
            maximum_bytes,
        };
        writer.push(VALUE_ABI_MAGIC)?;
        Ok(writer)
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), ValueAbiError> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.maximum_bytes)
        {
            return Err(ValueAbiError::TooLarge);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn tag(&mut self, tag: u8) -> Result<(), ValueAbiError> {
        self.push(&[tag])
    }

    fn length(&mut self, length: usize) -> Result<(), ValueAbiError> {
        let length = u32::try_from(length).map_err(|_| ValueAbiError::TooLarge)?;
        self.push(&length.to_le_bytes())
    }

    fn slice(&mut self, bytes: &[u8]) -> Result<(), ValueAbiError> {
        self.length(bytes.len())?;
        self.push(bytes)
    }

    fn committed(&mut self, value: &ConvexValue) -> Result<(), ValueAbiError> {
        match value {
            ConvexValue::Null => self.tag(NULL),
            ConvexValue::Boolean(false) => self.tag(FALSE),
            ConvexValue::Boolean(true) => self.tag(TRUE),
            ConvexValue::Float64(value) => {
                self.tag(FLOAT64)?;
                self.push(&value.to_bits().to_le_bytes())
            },
            ConvexValue::Int64(value) => {
                self.tag(INT64)?;
                self.push(&value.to_le_bytes())
            },
            ConvexValue::String(value) => {
                self.tag(STRING)?;
                self.slice(value.as_bytes())
            },
            ConvexValue::Bytes(value) => {
                self.tag(BYTES)?;
                self.slice(value)
            },
            ConvexValue::Array(values) => {
                self.tag(ARRAY)?;
                self.length(values.len())?;
                for value in values.iter() {
                    self.committed(value)?;
                }
                Ok(())
            },
            ConvexValue::Object(fields) => {
                self.tag(OBJECT)?;
                self.length(fields.len())?;
                for (name, value) in fields.iter() {
                    self.slice(name.as_bytes())?;
                    self.committed(value)?;
                }
                Ok(())
            },
        }
    }

    fn pending(&mut self, value: &PendingValue) -> Result<(), ValueAbiError> {
        match value {
            PendingValue::Concrete(value) => self.committed(value),
            PendingValue::CommitTs => self.tag(COMMIT_TS),
            PendingValue::Array { values, .. } => {
                self.tag(ARRAY)?;
                self.length(values.len())?;
                for value in values {
                    self.pending(value)?;
                }
                Ok(())
            },
            PendingValue::Object { fields, .. } => {
                self.tag(OBJECT)?;
                self.length(fields.len())?;
                for (name, value) in fields {
                    self.slice(name.as_bytes())?;
                    self.pending(value)?;
                }
                Ok(())
            },
        }
    }

    fn packed_document(&mut self, bytes: &[u8]) -> Result<(), ValueAbiError> {
        self.tag(PACKED_DOCUMENT_TAG)?;
        self.slice(bytes)
    }

    fn document(&mut self, document: DocumentResult<'_>) -> Result<(), ValueAbiError> {
        match document {
            DocumentResult::Packed(bytes) => self.packed_document(bytes),
            DocumentResult::Pending(value) => self.pending(value),
        }
    }

    fn document_collection(
        &mut self,
        documents: &[DocumentResult<'_>],
    ) -> Result<(), ValueAbiError> {
        self.tag(DOCUMENT_COLLECTION_TAG)?;
        self.length(documents.len())?;
        for document in documents {
            self.document(*document)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum DocumentResult<'a> {
    Packed(&'a [u8]),
    Pending(&'a PendingValue),
}

pub fn encode_committed(
    value: &ConvexValue,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.committed(value)?;
    Ok(writer.bytes)
}

pub fn encode_pending(
    value: &PendingValue,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.pending(value)?;
    Ok(writer.bytes)
}

/// Wrap an already packed document for host-to-guest delivery without decoding
/// it.
pub fn encode_packed_document(
    packed_bytes: &[u8],
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.packed_document(packed_bytes)?;
    Ok(writer.bytes)
}

/// Preserve each stored document's buffer when delivering a query collection.
pub fn encode_document_collection(
    documents: &[DocumentResult<'_>],
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.document_collection(documents)?;
    Ok(writer.bytes)
}

pub fn encode_query_stream_next(
    document: Option<DocumentResult<'_>>,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.tag(OBJECT)?;
    writer.length(2)?;
    writer.slice(b"done")?;
    writer.tag(if document.is_some() { FALSE } else { TRUE })?;
    writer.slice(b"value")?;
    match document {
        Some(document) => writer.document(document)?,
        None => writer.tag(NULL)?,
    }
    Ok(writer.bytes)
}

pub fn encode_query_page(
    documents: &[DocumentResult<'_>],
    is_done: bool,
    continue_cursor: &str,
    split_cursor: Option<&str>,
    page_status: Option<&str>,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ValueAbiError> {
    let mut writer = Writer::new(maximum_bytes)?;
    writer.tag(OBJECT)?;
    writer.length(5)?;
    writer.slice(b"continueCursor")?;
    writer.tag(STRING)?;
    writer.slice(continue_cursor.as_bytes())?;
    writer.slice(b"isDone")?;
    writer.tag(if is_done { TRUE } else { FALSE })?;
    writer.slice(b"page")?;
    writer.document_collection(documents)?;
    writer.slice(b"pageStatus")?;
    match page_status {
        Some(value) => {
            writer.tag(STRING)?;
            writer.slice(value.as_bytes())?;
        },
        None => writer.tag(NULL)?,
    }
    writer.slice(b"splitCursor")?;
    match split_cursor {
        Some(value) => {
            writer.tag(STRING)?;
            writer.slice(value.as_bytes())?;
        },
        None => writer.tag(NULL)?,
    }
    Ok(writer.bytes)
}

struct Reader<'a> {
    remaining: &'a [u8],
    allows_pending: bool,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], ValueAbiError> {
        let remaining = self.remaining;
        if length > remaining.len() {
            return Err(ValueAbiError::Malformed);
        }
        let (bytes, rest) = remaining.split_at(length);
        self.remaining = rest;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, ValueAbiError> {
        Ok(self.take(1)?[0])
    }

    fn length(&mut self) -> Result<usize, ValueAbiError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ValueAbiError::Malformed)?;
        Ok(u32::from_le_bytes(bytes) as usize)
    }

    fn slice(&mut self) -> Result<&'a [u8], ValueAbiError> {
        let length = self.length()?;
        self.take(length)
    }

    fn value(&mut self, nesting: usize) -> Result<PendingValue, ValueAbiError> {
        let value = match self.byte()? {
            NULL => PendingValue::Concrete(ConvexValue::Null),
            FALSE => PendingValue::Concrete(ConvexValue::Boolean(false)),
            TRUE => PendingValue::Concrete(ConvexValue::Boolean(true)),
            FLOAT64 => {
                let bits: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .map_err(|_| ValueAbiError::Malformed)?;
                PendingValue::Concrete(ConvexValue::Float64(f64::from_bits(u64::from_le_bytes(
                    bits,
                ))))
            },
            INT64 => {
                let bytes: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .map_err(|_| ValueAbiError::Malformed)?;
                PendingValue::Concrete(ConvexValue::Int64(i64::from_le_bytes(bytes)))
            },
            STRING => {
                let string =
                    std::str::from_utf8(self.slice()?).map_err(|_| ValueAbiError::InvalidValue)?;
                PendingValue::Concrete(ConvexValue::String(
                    ConvexString::try_from(string).map_err(|_| ValueAbiError::InvalidValue)?,
                ))
            },
            BYTES => PendingValue::Concrete(ConvexValue::Bytes(
                ConvexBytes::try_from(self.slice()?.to_vec())
                    .map_err(|_| ValueAbiError::InvalidValue)?,
            )),
            ARRAY => {
                if nesting >= MAX_NESTING {
                    return Err(ValueAbiError::InvalidValue);
                }
                let count = self.length()?;
                if count > MAX_ARRAY_LEN || count > self.remaining.len() {
                    return Err(ValueAbiError::InvalidValue);
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.value(nesting + 1)?);
                }
                PendingValue::array(values).map_err(|_| ValueAbiError::InvalidValue)?
            },
            OBJECT => {
                if nesting >= MAX_NESTING {
                    return Err(ValueAbiError::InvalidValue);
                }
                let count = self.length()?;
                if count > MAX_OBJECT_FIELDS || count > self.remaining.len() / 5 {
                    return Err(ValueAbiError::InvalidValue);
                }
                let mut fields = BTreeMap::new();
                for _ in 0..count {
                    let name = std::str::from_utf8(self.slice()?)
                        .map_err(|_| ValueAbiError::InvalidValue)?
                        .parse::<FieldName>()
                        .map_err(|_| ValueAbiError::InvalidValue)?;
                    if fields.contains_key(&name) {
                        return Err(ValueAbiError::InvalidValue);
                    }
                    fields.insert(name, self.value(nesting + 1)?);
                }
                PendingValue::object(fields).map_err(|_| ValueAbiError::InvalidValue)?
            },
            COMMIT_TS if self.allows_pending => PendingValue::CommitTs,
            _ => return Err(ValueAbiError::InvalidValue),
        };
        Ok(value)
    }

    fn patch(&mut self) -> Result<BTreeMap<FieldName, Option<PendingValue>>, ValueAbiError> {
        if self.byte()? != OBJECT {
            return Err(ValueAbiError::InvalidValue);
        }
        let count = self.length()?;
        if count > MAX_OBJECT_FIELDS || count > self.remaining.len() / 5 {
            return Err(ValueAbiError::InvalidValue);
        }
        let mut fields = BTreeMap::new();
        for _ in 0..count {
            let name = std::str::from_utf8(self.slice()?)
                .map_err(|_| ValueAbiError::InvalidValue)?
                .parse::<FieldName>()
                .map_err(|_| ValueAbiError::InvalidValue)?;
            if fields.contains_key(&name) {
                return Err(ValueAbiError::InvalidValue);
            }
            let value = if self.remaining.first() == Some(&PATCH_DELETE_TAG) {
                self.byte()?;
                None
            } else {
                Some(self.value(1)?)
            };
            fields.insert(name, value);
        }
        Ok(fields)
    }
}

fn decode(
    bytes: &[u8],
    maximum_bytes: usize,
    allows_pending: bool,
) -> Result<PendingValue, ValueAbiError> {
    if bytes.len() > maximum_bytes {
        return Err(ValueAbiError::TooLarge);
    }
    let mut reader = Reader {
        remaining: bytes,
        allows_pending,
    };
    if reader.take(VALUE_ABI_MAGIC.len())? != VALUE_ABI_MAGIC {
        return Err(ValueAbiError::Malformed);
    }
    let value = reader.value(0)?;
    if !reader.remaining.is_empty() {
        return Err(ValueAbiError::Malformed);
    }
    Ok(value)
}

pub fn decode_committed(bytes: &[u8], maximum_bytes: usize) -> Result<ConvexValue, ValueAbiError> {
    decode(bytes, maximum_bytes, false)?
        .try_into_concrete()
        .map_err(|_| ValueAbiError::InvalidValue)
}

pub fn decode_pending(bytes: &[u8], maximum_bytes: usize) -> Result<PendingValue, ValueAbiError> {
    decode(bytes, maximum_bytes, true)
}

pub fn decode_patch(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<BTreeMap<FieldName, Option<PendingValue>>, ValueAbiError> {
    if bytes.len() > maximum_bytes {
        return Err(ValueAbiError::TooLarge);
    }
    let mut reader = Reader {
        remaining: bytes,
        allows_pending: true,
    };
    if reader.take(VALUE_ABI_MAGIC.len())? != VALUE_ABI_MAGIC {
        return Err(ValueAbiError::Malformed);
    }
    let fields = reader.patch()?;
    if !reader.remaining.is_empty() {
        return Err(ValueAbiError::Malformed);
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_little_endian_values_preserve_int64_and_negative_zero() {
        let int64 = ConvexValue::Int64(i64::MIN);
        let bytes = encode_committed(&int64, 64).unwrap();
        assert_eq!(
            bytes,
            [b'C', b'V', b'A', b'1', INT64, 0, 0, 0, 0, 0, 0, 0, 128]
        );
        assert_eq!(decode_committed(&bytes, bytes.len()).unwrap(), int64);

        let negative_zero = ConvexValue::Float64(-0.0);
        let bytes = encode_committed(&negative_zero, 64).unwrap();
        assert_eq!(
            bytes,
            [b'C', b'V', b'A', b'1', FLOAT64, 0, 0, 0, 0, 0, 0, 0, 128]
        );
        let ConvexValue::Float64(decoded) = decode_committed(&bytes, bytes.len()).unwrap() else {
            panic!("decoded a different value kind");
        };
        assert_eq!(decoded.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn pending_values_and_invalid_frames_keep_distinct_meanings() {
        let pending = PendingValue::object(BTreeMap::from([(
            "created".parse().unwrap(),
            PendingValue::CommitTs,
        )]))
        .unwrap();
        let encoded = encode_pending(&pending, 128).unwrap();
        assert_eq!(decode_pending(&encoded, 128).unwrap(), pending);
        assert_eq!(
            decode_committed(&encoded, 128),
            Err(ValueAbiError::InvalidValue)
        );

        let duplicate = [
            b'C', b'V', b'A', b'1', OBJECT, 2, 0, 0, 0, 1, 0, 0, 0, b'x', NULL, 1, 0, 0, 0, b'x',
            NULL,
        ];
        assert_eq!(
            decode_committed(&duplicate, duplicate.len()),
            Err(ValueAbiError::InvalidValue)
        );
        assert_eq!(
            decode_committed(&[b'C', b'V', b'A', b'1', INT64, 0], duplicate.len()),
            Err(ValueAbiError::Malformed)
        );
        assert_eq!(
            decode_committed(&duplicate, duplicate.len() - 1),
            Err(ValueAbiError::TooLarge)
        );

        let packed = [0, 255, 128, 1];
        let frame = encode_packed_document(&packed, 64).unwrap();
        assert_eq!(
            frame,
            [
                b'C',
                b'V',
                b'A',
                b'1',
                PACKED_DOCUMENT_TAG,
                4,
                0,
                0,
                0,
                0,
                255,
                128,
                1
            ]
        );
        assert_eq!(
            decode_committed(&frame, 64),
            Err(ValueAbiError::InvalidValue)
        );

        let pending = PendingValue::CommitTs;
        let collection = encode_document_collection(
            &[
                DocumentResult::Packed(&packed),
                DocumentResult::Pending(&pending),
            ],
            64,
        )
        .unwrap();
        assert_eq!(
            collection,
            [
                b'C',
                b'V',
                b'A',
                b'1',
                DOCUMENT_COLLECTION_TAG,
                2,
                0,
                0,
                0,
                PACKED_DOCUMENT_TAG,
                4,
                0,
                0,
                0,
                0,
                255,
                128,
                1,
                COMMIT_TS,
            ]
        );
    }
}
