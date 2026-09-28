use value::{
    heap_size::HeapSize,
    sha256::{
        Sha256,
        Sha256Digest,
    },
};

use crate::query::{
    Cursor,
    CursorPosition,
};

/// A journal to keep track of decisions made while executing a query function.
///
/// The query journal is synced to the client and re-used whenever a query is
/// re-executed (even if the client recconects to a new backend). This can
/// ensure that re-executions make the same decisions as the initial one did.
///
/// Invariant:
/// At timestamp t, if a query function q produces:
/// `q(arguments, prev_journal) -> (result, next_journal)`
/// then at t,
/// `q(arguments, next_journal) -> (result, next_journal)`
/// Reusing a journal as an input at the same timestamp should
/// produce the same result and the same journal.
///
/// Because this journal is synced to the client, keep its size small!
/// The serialized size is tested in `broker.rs:test_query_journal_size`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QueryJournal {
    /// If this query function ran a paginated database query, store a cursor
    /// for the end of the query so we can continue to sync to the same point
    /// if this function is re-executed.
    pub end_cursor: Option<Cursor>,
}

/// A privacy-safe identity for comparing the logical state of a query journal.
///
/// This retains the cursor state needed for equality while hashing the query
/// fingerprint and index key instead of retaining their raw bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryJournalLogicalIdentity {
    None,
    End {
        query_fingerprint_sha256: Sha256Digest,
    },
    After {
        query_fingerprint_sha256: Sha256Digest,
        after_key_sha256: Sha256Digest,
    },
}

impl QueryJournal {
    pub fn new() -> QueryJournal {
        QueryJournal { end_cursor: None }
    }

    pub fn logical_identity(&self) -> QueryJournalLogicalIdentity {
        let Some(cursor) = &self.end_cursor else {
            return QueryJournalLogicalIdentity::None;
        };
        let query_fingerprint_sha256 = Sha256::hash(&cursor.query_fingerprint);
        match &cursor.position {
            CursorPosition::End => QueryJournalLogicalIdentity::End {
                query_fingerprint_sha256,
            },
            CursorPosition::After(key) => QueryJournalLogicalIdentity::After {
                query_fingerprint_sha256,
                after_key_sha256: Sha256::hash(key),
            },
        }
    }
}

impl HeapSize for QueryJournal {
    fn heap_size(&self) -> usize {
        match &self.end_cursor {
            Some(cursor) => cursor.heap_size(),
            None => 0,
        }
    }
}

impl From<QueryJournal> for pb::convex_query_journal::QueryJournal {
    fn from(QueryJournal { end_cursor }: QueryJournal) -> Self {
        Self {
            cursor: end_cursor.map(pb::convex_cursor::Cursor::from),
        }
    }
}

impl TryFrom<pb::convex_query_journal::QueryJournal> for QueryJournal {
    type Error = anyhow::Error;

    fn try_from(
        pb::convex_query_journal::QueryJournal { cursor }: pb::convex_query_journal::QueryJournal,
    ) -> anyhow::Result<Self> {
        let end_cursor = cursor.map(Cursor::try_from).transpose()?;
        Ok(QueryJournal { end_cursor })
    }
}

#[cfg(test)]
mod tests {
    use value::sha256::Sha256;

    use super::{
        Cursor,
        CursorPosition,
        QueryJournal,
        QueryJournalLogicalIdentity,
    };
    use crate::index::IndexKeyBytes;

    #[test]
    fn logical_identity_hashes_cursor_material_without_collapsing_positions() {
        assert_eq!(
            QueryJournal::new().logical_identity(),
            QueryJournalLogicalIdentity::None
        );

        let query_fingerprint = b"query fingerprint".to_vec();
        assert_eq!(
            QueryJournal {
                end_cursor: Some(Cursor {
                    position: CursorPosition::End,
                    query_fingerprint: query_fingerprint.clone(),
                }),
            }
            .logical_identity(),
            QueryJournalLogicalIdentity::End {
                query_fingerprint_sha256: Sha256::hash(&query_fingerprint),
            }
        );

        let after_key = b"after key".to_vec();
        assert_eq!(
            QueryJournal {
                end_cursor: Some(Cursor {
                    position: CursorPosition::After(IndexKeyBytes(after_key.clone())),
                    query_fingerprint: query_fingerprint.clone(),
                }),
            }
            .logical_identity(),
            QueryJournalLogicalIdentity::After {
                query_fingerprint_sha256: Sha256::hash(&query_fingerprint),
                after_key_sha256: Sha256::hash(&after_key),
            }
        );
    }
}
