pub mod consumer;
pub mod db_types;
pub mod error;
pub mod server;
pub mod storage;
pub mod storage_fjall;
pub mod storage_mem;
pub mod store_types;

use crate::error::BatchInsertError;
use cardinality_estimator::CardinalityEstimator;
use error::FirehoseEventError;
use jetstream::events::{CommitEvent, CommitOp, Cursor};
use jetstream::exports::{Did, Nsid, RecordKey};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct CollectionCommits<const LIMIT: usize> {
    pub total_seen: usize,
    pub dids_estimate: CardinalityEstimator<Did>,
    pub commits: Vec<UFOsCommit>,
    head: usize,
    non_creates: usize,
}

impl<const LIMIT: usize> CollectionCommits<LIMIT> {
    fn advance_head(&mut self) {
        self.head += 1;
        if self.head > LIMIT {
            self.head = 0;
        }
    }
    pub fn truncating_insert(&mut self, commit: UFOsCommit) -> Result<(), BatchInsertError> {
        if self.non_creates == LIMIT {
            return Err(BatchInsertError::BatchFull(commit));
        }
        let did = commit.did.clone();
        let is_create = commit.action.is_create();
        if self.commits.len() < LIMIT {
            self.commits.push(commit);
            if self.commits.capacity() > LIMIT {
                self.commits.shrink_to(LIMIT); // save mem?????? maybe??
            }
        } else {
            let head_started_at = self.head;
            loop {
                let candidate = self
                    .commits
                    .get_mut(self.head)
                    .ok_or(BatchInsertError::BatchOverflow(self.head))?;
                if candidate.action.is_create() {
                    *candidate = commit;
                    break;
                }
                self.advance_head();
                if self.head == head_started_at {
                    return Err(BatchInsertError::BatchForever);
                }
            }
        }

        if is_create {
            self.total_seen += 1;
            self.dids_estimate.insert(&did);
        } else {
            self.non_creates += 1;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeleteAccount {
    pub did: Did,
    pub cursor: Cursor,
}

#[derive(Debug, Clone)]
pub enum CommitAction {
    Put(PutAction),
    Cut,
}
impl CommitAction {
    pub fn is_create(&self) -> bool {
        match self {
            CommitAction::Put(PutAction { is_update, .. }) => !is_update,
            CommitAction::Cut => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutAction {
    record: Box<RawValue>,
    is_update: bool,
}

#[derive(Debug, Clone)]
pub struct UFOsCommit {
    cursor: Cursor,
    did: Did,
    rkey: RecordKey,
    rev: String,
    action: CommitAction,
}

#[derive(Debug, Clone, Serialize)]
pub struct UFOsRecord {
    pub cursor: Cursor,
    pub did: Did,
    pub collection: Nsid,
    pub rkey: RecordKey,
    pub rev: String,
    // TODO: cid?
    pub record: Box<RawValue>,
    pub is_update: bool,
}

impl UFOsCommit {
    pub fn from_commit_info(
        commit: CommitEvent,
        did: Did,
        cursor: Cursor,
    ) -> Result<(Self, Nsid), FirehoseEventError> {
        let action = match commit.operation {
            CommitOp::Delete => CommitAction::Cut,
            cru => CommitAction::Put(PutAction {
                record: commit.record.ok_or(FirehoseEventError::CruMissingRecord)?,
                is_update: cru == CommitOp::Update,
            }),
        };
        let batched = Self {
            cursor,
            did,
            rkey: commit.rkey,
            rev: commit.rev,
            action,
        };
        Ok((batched, commit.collection))
    }
}

#[derive(Debug, Default, Clone)]
pub struct EventBatch<const LIMIT: usize> {
    pub commits_by_nsid: HashMap<Nsid, CollectionCommits<LIMIT>>,
    pub account_removes: Vec<DeleteAccount>,
}

impl<const LIMIT: usize> EventBatch<LIMIT> {
    pub fn insert_commit_by_nsid(
        &mut self,
        collection: &Nsid,
        commit: UFOsCommit,
        max_collections: usize,
    ) -> Result<(), BatchInsertError> {
        let map = &mut self.commits_by_nsid;
        if !map.contains_key(collection) && map.len() >= max_collections {
            return Err(BatchInsertError::BatchFull(commit));
        }
        map.entry(collection.clone())
            .or_default()
            .truncating_insert(commit)?;
        Ok(())
    }
    pub fn total_records(&self) -> usize {
        self.commits_by_nsid.values().map(|v| v.commits.len()).sum()
    }
    pub fn total_seen(&self) -> usize {
        self.commits_by_nsid.values().map(|v| v.total_seen).sum()
    }
    pub fn total_collections(&self) -> usize {
        self.commits_by_nsid.len()
    }
    pub fn account_removes(&self) -> usize {
        self.account_removes.len()
    }
    pub fn estimate_dids(&self) -> usize {
        let mut estimator = CardinalityEstimator::<Did>::new();
        for commits in self.commits_by_nsid.values() {
            estimator.merge(&commits.dids_estimate);
        }
        estimator.estimate()
    }
    pub fn latest_cursor(&self) -> Option<Cursor> {
        let mut oldest = Cursor::from_start();
        for commits in self.commits_by_nsid.values() {
            for commit in &commits.commits {
                if commit.cursor > oldest {
                    oldest = commit.cursor;
                }
            }
        }
        if let Some(del) = self.account_removes.last() {
            if del.cursor > oldest {
                oldest = del.cursor;
            }
        }
        if oldest > Cursor::from_start() {
            Some(oldest)
        } else {
            None
        }
    }
    pub fn is_empty(&self) -> bool {
        self.commits_by_nsid.is_empty() && self.account_removes.is_empty()
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub enum ConsumerInfo {
    Jetstream {
        endpoint: String,
        started_at: u64,
        latest_cursor: Option<u64>,
    },
}

#[derive(Debug, Default, PartialEq, Serialize, JsonSchema)]
pub struct TopCollections {
    total_records: u64,
    dids_estimate: u64,
    nsid_child_segments: HashMap<String, TopCollections>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncating_insert_truncates() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(100),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
            rev: "rev-asdf".to_string(),
            action: CommitAction::Put(PutAction {
                record: RawValue::from_string("{}".to_string())?,
                is_update: false,
            }),
        })?;

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(101),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
            rev: "rev-asdg".to_string(),
            action: CommitAction::Put(PutAction {
                record: RawValue::from_string("{}".to_string())?,
                is_update: false,
            }),
        })?;

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(102),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
            rev: "rev-asdh".to_string(),
            action: CommitAction::Put(PutAction {
                record: RawValue::from_string("{}".to_string())?,
                is_update: false,
            }),
        })?;

        assert_eq!(commits.total_seen, 3);
        assert_eq!(commits.dids_estimate.estimate(), 1);
        assert_eq!(commits.commits.len(), 2);

        let mut found_first = false;
        let mut found_last = false;
        for commit in commits.commits {
            match commit.rev.as_ref() {
                "rev-asdf" => {
                    found_first = true;
                }
                "rev-asdh" => {
                    found_last = true;
                }
                _ => {}
            }
        }
        assert!(!found_first);
        assert!(found_last);

        Ok(())
    }

    #[test]
    fn test_truncating_insert_does_not_truncate_deletes() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(100),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
            rev: "rev-asdf".to_string(),
            action: CommitAction::Cut,
        })?;

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(101),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
            rev: "rev-asdg".to_string(),
            action: CommitAction::Put(PutAction {
                record: RawValue::from_string("{}".to_string())?,
                is_update: false,
            }),
        })?;

        commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(102),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
            rev: "rev-asdh".to_string(),
            action: CommitAction::Put(PutAction {
                record: RawValue::from_string("{}".to_string())?,
                is_update: false,
            }),
        })?;

        assert_eq!(commits.total_seen, 2);
        assert_eq!(commits.dids_estimate.estimate(), 1);
        assert_eq!(commits.commits.len(), 2);

        let mut found_first = false;
        let mut found_last = false;
        let mut found_delete = false;
        for commit in commits.commits {
            match commit.rev.as_ref() {
                "rev-asdg" => {
                    found_first = true;
                }
                "rev-asdh" => {
                    found_last = true;
                }
                _ => {}
            }
            if let CommitAction::Cut = commit.action {
                found_delete = true;
            }
        }
        assert!(!found_first);
        assert!(found_last);
        assert!(found_delete);

        Ok(())
    }

    #[test]
    fn test_truncating_insert_maxes_out_deletes() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits
            .truncating_insert(UFOsCommit {
                cursor: Cursor::from_raw_u64(100),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
                rev: "rev-asdf".to_string(),
                action: CommitAction::Cut,
            })
            .unwrap();

        // this create will just be discarded
        commits
            .truncating_insert(UFOsCommit {
                cursor: Cursor::from_raw_u64(80),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-zzz".to_string()).unwrap(),
                rev: "rev-asdzzz".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            })
            .unwrap();

        commits
            .truncating_insert(UFOsCommit {
                cursor: Cursor::from_raw_u64(101),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
                rev: "rev-asdg".to_string(),
                action: CommitAction::Cut,
            })
            .unwrap();

        let res = commits.truncating_insert(UFOsCommit {
            cursor: Cursor::from_raw_u64(102),
            did: Did::new("did:plc:whatever".to_string()).unwrap(),
            rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
            rev: "rev-asdh".to_string(),
            action: CommitAction::Cut,
        });

        assert!(res.is_err());
        let overflowed = match res {
            Err(BatchInsertError::BatchFull(c)) => c,
            e => panic!("expected overflow but a different error happened: {e:?}"),
        };
        assert_eq!(overflowed.rev, "rev-asdh");

        Ok(())
    }
}
