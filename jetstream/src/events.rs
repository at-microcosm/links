use std::time::{
    Duration,
    SystemTime,
    SystemTimeError,
    UNIX_EPOCH,
};

use chrono::Utc;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::value::RawValue;

use crate::exports;

/// Opaque wrapper for the time_us cursor used by jetstream
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, PartialOrd)]
pub struct Cursor(u64);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct JetstreamEvent {
    #[serde(rename = "time_us")]
    pub cursor: Cursor,
    pub did: exports::Did,
    pub kind: EventKind,
    pub commit: Option<CommitEvent>,
    pub identity: Option<IdentityEvent>,
    pub account: Option<AccountEvent>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Commit,
    Identity,
    Account,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CommitEvent {
    pub collection: exports::Nsid,
    pub rkey: exports::RecordKey,
    pub rev: String,
    pub operation: CommitOp,
    pub record: Option<Box<RawValue>>,
    pub cid: Option<exports::Cid>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CommitOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IdentityEvent {
    pub did: exports::Did,
    pub handle: Option<exports::Handle>,
    pub seq: u64,
    pub time: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct AccountEvent {
    pub active: bool,
    pub did: exports::Did,
    pub seq: u64,
    pub time: chrono::DateTime<Utc>,
    pub status: Option<String>,
}

impl Cursor {
    /// Get a cursor that will consume all available jetstream replay
    ///
    /// This sets the cursor to zero.
    ///
    /// Jetstream instances typically only have a few days of replay.
    pub fn from_start() -> Self {
        Self(0)
    }
    /// Get a cursor for a specific time
    ///
    /// Panics: if t is older than the unix epoch: Jan 1, 1970.
    ///
    /// If you want to receive all available jetstream replay (typically a few days), use
    /// .from_start()
    ///
    /// Warning: this exploits the internal implementation detail of jetstream cursors
    /// being ~microsecond timestamps.
    pub fn at(t: SystemTime) -> Self {
        let unix_dt = t
            .duration_since(UNIX_EPOCH)
            .expect("cannot set jetstream cursor earlier than unix epoch");
        Self(unix_dt.as_micros() as u64)
    }
    /// Get a cursor rewound from now by this amount
    ///
    /// Panics: if d is greater than the time since the unix epoch: Jan 1, 1970.
    ///
    /// Jetstream instances typically only have a few days of replay.
    ///
    /// Warning: this exploits the internal implementation detail of jetstream cursors
    /// being ~microsecond timestamps.
    pub fn back_by(d: Duration) -> Self {
        Self::at(SystemTime::now() - d)
    }
    /// Get a Cursor from a raw u64
    ///
    /// For example, from a jetstream event's `time_us` field.
    pub fn from_raw_u64(time_us: u64) -> Self {
        Self(time_us)
    }
    /// Get the raw u64 value from this cursor.
    pub fn to_raw_u64(&self) -> u64 {
        self.0
    }
    /// Format the cursor value for use in a jetstream connection url querystring
    pub fn to_jetstream(&self) -> String {
        self.0.to_string()
    }
    /// Compute the time span since an earlier cursor or [SystemTime]
    ///
    /// Warning: this exploits the internal implementation detail of jetstream cursors
    /// being ~microsecond timestamps.
    pub fn duration_since(
        &self,
        earlier: impl Into<SystemTime>,
    ) -> Result<Duration, SystemTimeError> {
        let t: SystemTime = self.into();
        t.duration_since(earlier.into())
    }
    /// Compute the age of the cursor vs the local clock
    ///
    /// Warning: this exploits the internal implementation detail of jetstream cursors
    pub fn elapsed(&self) -> Result<Duration, SystemTimeError> {
        let t: SystemTime = self.into();
        t.elapsed()
    }
    /// Get the immediate next cursor value
    ///
    /// This is possible for the implementation of jetstream cursors
    pub fn next(&self) -> Cursor {
        Self(self.0 + 1)
    }
}

impl From<&Cursor> for SystemTime {
    /// Convert a cursor directly to a [SystemTime]
    ///
    /// Warning: this exploits the internal implementation detail of jetstream cursors
    /// being ~microsecond timestamps.
    fn from(c: &Cursor) -> Self {
        UNIX_EPOCH + Duration::from_micros(c.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_commit_event() -> anyhow::Result<()> {
        let json = r#"{
            "rev":"3llrdsginou2i",
            "operation":"create",
            "collection":"app.bsky.feed.post",
            "rkey":"3llrdsglqdc2s",
            "cid": "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy",
            "record": {"$type":"app.bsky.feed.post","createdAt":"2025-04-01T16:58:06.154Z","langs":["en"],"text":"I wish apirl 1st would stop existing lol"}
        }"#;
        let commit: CommitEvent = serde_json::from_str(json)?;
        assert_eq!(
            commit.cid.unwrap(),
            "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy".parse()?
        );
        assert_eq!(
            commit.record.unwrap().get(),
            r#"{"$type":"app.bsky.feed.post","createdAt":"2025-04-01T16:58:06.154Z","langs":["en"],"text":"I wish apirl 1st would stop existing lol"}"#
        );
        Ok(())
    }

    #[test]
    fn test_parse_whole_event() -> anyhow::Result<()> {
        let json = r#"{"did":"did:plc:ai3dzf35cth7s3st7n7jsd7r","time_us":1743526687419798,"kind":"commit","commit":{"rev":"3llrdsginou2i","operation":"create","collection":"app.bsky.feed.post","rkey":"3llrdsglqdc2s","record":{"$type":"app.bsky.feed.post","createdAt":"2025-04-01T16:58:06.154Z","langs":["en"],"text":"I wish apirl 1st would stop existing lol"},"cid":"bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy"}}"#;
        let event: JetstreamEvent = serde_json::from_str(json)?;
        assert_eq!(event.kind, EventKind::Commit);
        assert!(event.commit.is_some());
        let commit = event.commit.unwrap();
        assert_eq!(
            commit.cid.unwrap(),
            "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy".parse()?
        );
        assert_eq!(
            commit.record.unwrap().get(),
            r#"{"$type":"app.bsky.feed.post","createdAt":"2025-04-01T16:58:06.154Z","langs":["en"],"text":"I wish apirl 1st would stop existing lol"}"#
        );
        Ok(())
    }
}
