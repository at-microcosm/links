use crate::db_types::{
    DbBytes, DbConcat, DbStaticStr, EncodingError, SerdeBytes, StaticStr, UseBincodePlz,
};
use crate::{Cursor, Did, Nsid, PutAction, RecordKey, UFOsCommit};
use bincode::{Decode, Encode};
use cardinality_estimator::CardinalityEstimator;
use std::ops::Range;

/// key format: ["js_cursor"]
#[derive(Debug, PartialEq)]
pub struct JetstreamCursorKey {}
impl StaticStr for JetstreamCursorKey {
    fn static_str() -> &'static str {
        "js_cursor"
    }
}
pub type JetstreamCursorValue = Cursor;

/// key format: ["rollup_cursor"]
#[derive(Debug, PartialEq)]
pub struct NewRollupCursorKey {}
impl StaticStr for NewRollupCursorKey {
    fn static_str() -> &'static str {
        "rollup_cursor"
    }
}
// pub type NewRollupCursorKey = DbStaticStr<_NewRollupCursorKey>;
/// value format: [rollup_cursor(Cursor)|collection(Nsid)]
pub type NewRollupCursorValue = Cursor;

/// key format: ["js_endpoint"]
#[derive(Debug, PartialEq)]
pub struct TakeoffKey {}
impl StaticStr for TakeoffKey {
    fn static_str() -> &'static str {
        "takeoff"
    }
}
pub type TakeoffValue = Cursor;

/// key format: ["js_endpoint"]
#[derive(Debug, PartialEq)]
pub struct JetstreamEndpointKey {}
impl StaticStr for JetstreamEndpointKey {
    fn static_str() -> &'static str {
        "js_endpoint"
    }
}
#[derive(Debug, PartialEq)]
pub struct JetstreamEndpointValue(pub String);
/// String wrapper for jetstream endpoint value
///
/// Warning: this is a non-terminating byte representation of a string: it cannot be used in prefix position of DbConcat
impl DbBytes for JetstreamEndpointValue {
    // TODO: maybe make a helper type in db_types
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.0.as_bytes().to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let s = std::str::from_utf8(bytes)?.to_string();
        Ok((Self(s), bytes.len()))
    }
}

pub type NsidRecordFeedKey = DbConcat<Nsid, Cursor>;
impl NsidRecordFeedKey {
    pub fn collection(&self) -> &Nsid {
        &self.prefix
    }
    pub fn cursor(&self) -> Cursor {
        self.suffix
    }
}
pub type NsidRecordFeedVal = DbConcat<Did, DbConcat<RecordKey, String>>;
impl NsidRecordFeedVal {
    pub fn did(&self) -> &Did {
        &self.prefix
    }
    pub fn rkey(&self) -> &RecordKey {
        &self.suffix.prefix
    }
    pub fn rev(&self) -> &str {
        &self.suffix.suffix
    }
}
impl From<(&Did, &RecordKey, &str)> for NsidRecordFeedVal {
    fn from((did, rkey, rev): (&Did, &RecordKey, &str)) -> Self {
        Self::from_pair(
            did.clone(),
            DbConcat::from_pair(rkey.clone(), rev.to_string()),
        )
    }
}

pub type RecordLocationKey = DbConcat<Did, DbConcat<Nsid, RecordKey>>;
impl RecordLocationKey {
    pub fn did(&self) -> &Did {
        &self.prefix
    }
    pub fn collection(&self) -> &Nsid {
        &self.suffix.prefix
    }
    pub fn rkey(&self) -> &RecordKey {
        &self.suffix.suffix
    }
}
impl From<(&UFOsCommit, &Nsid)> for RecordLocationKey {
    fn from((commit, collection): (&UFOsCommit, &Nsid)) -> Self {
        Self::from_pair(
            commit.did.clone(),
            DbConcat::from_pair(collection.clone(), commit.rkey.clone()),
        )
    }
}
impl From<(&NsidRecordFeedKey, &NsidRecordFeedVal)> for RecordLocationKey {
    fn from((key, val): (&NsidRecordFeedKey, &NsidRecordFeedVal)) -> Self {
        Self::from_pair(
            val.did().clone(),
            DbConcat::from_pair(key.collection().clone(), val.rkey().clone()),
        )
    }
}

#[derive(Debug, PartialEq, Encode, Decode)]
pub struct RecordLocationMeta {
    cursor: u64, // ugh no bincode impl
    pub is_update: bool,
    pub rev: String,
}
impl RecordLocationMeta {
    pub fn cursor(&self) -> Cursor {
        Cursor::from_raw_u64(self.cursor)
    }
}
impl UseBincodePlz for RecordLocationMeta {}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordRawValue(Vec<u8>);
impl DbBytes for RecordRawValue {
    fn to_db_bytes(&self) -> Result<std::vec::Vec<u8>, EncodingError> {
        self.0.to_db_bytes()
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (v, n) = DbBytes::from_db_bytes(bytes)?;
        Ok((Self(v), n))
    }
}
impl From<Box<serde_json::value::RawValue>> for RecordRawValue {
    fn from(v: Box<serde_json::value::RawValue>) -> Self {
        Self(v.get().into())
    }
}
impl TryFrom<RecordRawValue> for Box<serde_json::value::RawValue> {
    type Error = EncodingError;
    fn try_from(rrv: RecordRawValue) -> Result<Self, Self::Error> {
        let s = String::from_utf8(rrv.0)?;
        let rv = serde_json::value::RawValue::from_string(s)?;
        Ok(rv)
    }
}

pub type RecordLocationVal = DbConcat<RecordLocationMeta, RecordRawValue>;
impl From<(Cursor, &str, PutAction)> for RecordLocationVal {
    fn from((cursor, rev, put): (Cursor, &str, PutAction)) -> Self {
        let meta = RecordLocationMeta {
            cursor: cursor.to_raw_u64(),
            is_update: put.is_update,
            rev: rev.to_string(),
        };
        Self::from_pair(meta, put.record.into())
    }
}

#[derive(Debug, PartialEq)]
pub struct _LiveRecordsStaticStr {}
impl StaticStr for _LiveRecordsStaticStr {
    fn static_str() -> &'static str {
        "live_counts"
    }
}

type LiveCountsStaticPrefix = DbStaticStr<_LiveRecordsStaticStr>;
type LiveCountsCursorPrefix = DbConcat<LiveCountsStaticPrefix, Cursor>;
pub type LiveCountsKey = DbConcat<LiveCountsCursorPrefix, Nsid>;
impl LiveCountsKey {
    pub fn range_from_cursor(cursor: Cursor) -> Result<Range<Vec<u8>>, EncodingError> {
        let prefix = LiveCountsCursorPrefix::from_pair(Default::default(), cursor);
        prefix.range_to_prefix_end()
    }
    pub fn cursor(&self) -> Cursor {
        self.prefix.suffix
    }
    pub fn collection(&self) -> &Nsid {
        &self.suffix
    }
}
impl From<(Cursor, &Nsid)> for LiveCountsKey {
    fn from((cursor, collection): (Cursor, &Nsid)) -> Self {
        Self::from_pair(
            LiveCountsCursorPrefix::from_pair(Default::default(), cursor),
            collection.clone(),
        )
    }
}
#[derive(Debug, PartialEq, Decode, Encode)]
pub struct TotalRecordsValue(pub u64);
impl UseBincodePlz for TotalRecordsValue {}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EstimatedDidsValue(pub CardinalityEstimator<Did>);
impl SerdeBytes for EstimatedDidsValue {}
impl DbBytes for EstimatedDidsValue {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(vec![1, 2, 3])
        // SerdeBytes::to_bytes(self)
    }
    fn from_db_bytes(_bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok((Self(CardinalityEstimator::new()), 3))
        // SerdeBytes::from_bytes(bytes)
    }
}

pub type CountsValue = DbConcat<TotalRecordsValue, EstimatedDidsValue>;
impl CountsValue {
    pub fn new(total: u64, dids: CardinalityEstimator<Did>) -> Self {
        Self {
            prefix: TotalRecordsValue(total),
            suffix: EstimatedDidsValue(dids),
        }
    }
    pub fn records(&self) -> u64 {
        self.prefix.0
    }
    pub fn dids(&self) -> &CardinalityEstimator<Did> {
        &self.suffix.0
    }
    pub fn merge(&mut self, other: &Self) {
        self.prefix.0 += other.records();
        self.suffix.0.merge(other.dids());
    }
}
impl Default for CountsValue {
    fn default() -> Self {
        Self {
            prefix: TotalRecordsValue(0),
            suffix: EstimatedDidsValue(CardinalityEstimator::new()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct _DeleteAccountStaticStr {}
impl StaticStr for _DeleteAccountStaticStr {
    fn static_str() -> &'static str {
        "delete_acount"
    }
}
pub type DeleteAccountStaticPrefix = DbStaticStr<_DeleteAccountStaticStr>;
pub type DeleteAccountQueueKey = DbConcat<DeleteAccountStaticPrefix, Cursor>;
impl DeleteAccountQueueKey {
    pub fn new(cursor: Cursor) -> Self {
        Self::from_pair(Default::default(), cursor)
    }
}
pub type DeleteAccountQueueVal = Did;

#[derive(Debug, PartialEq)]
pub struct _HourlyRollupStaticStr {}
impl StaticStr for _HourlyRollupStaticStr {
    fn static_str() -> &'static str {
        "hourly_counts"
    }
}
pub type HourlyRollupStaticPrefix = DbStaticStr<_HourlyRollupStaticStr>;
pub type HourlyRollupKey = DbConcat<DbConcat<HourlyRollupStaticPrefix, HourTruncatedCursor>, Nsid>;
impl HourlyRollupKey {
    pub fn new(hourly_cursor: HourTruncatedCursor, nsid: &Nsid) -> Self {
        Self::from_pair(
            DbConcat::from_pair(Default::default(), hourly_cursor),
            nsid.clone(),
        )
    }
}
pub type HourlyRollupVal = CountsValue;

#[derive(Debug, PartialEq)]
pub struct _WeeklyRollupStaticStr {}
impl StaticStr for _WeeklyRollupStaticStr {
    fn static_str() -> &'static str {
        "weekly_counts"
    }
}
pub type WeeklyRollupStaticPrefix = DbStaticStr<_WeeklyRollupStaticStr>;
pub type WeeklyRollupKey = DbConcat<DbConcat<WeeklyRollupStaticPrefix, WeekTruncatedCursor>, Nsid>;
impl WeeklyRollupKey {
    pub fn new(weekly_cursor: WeekTruncatedCursor, nsid: &Nsid) -> Self {
        Self::from_pair(
            DbConcat::from_pair(Default::default(), weekly_cursor),
            nsid.clone(),
        )
    }
}
pub type WeeklyRollupVal = CountsValue;

#[derive(Debug, PartialEq)]
pub struct _AllTimeRollupStaticStr {}
impl StaticStr for _AllTimeRollupStaticStr {
    fn static_str() -> &'static str {
        "ever_counts"
    }
}
pub type AllTimeRollupStaticPrefix = DbStaticStr<_AllTimeRollupStaticStr>;
pub type AllTimeRollupKey = DbConcat<AllTimeRollupStaticPrefix, Nsid>;
impl AllTimeRollupKey {
    pub fn new(nsid: &Nsid) -> Self {
        Self::from_pair(Default::default(), nsid.clone())
    }
    pub fn collection(&self) -> &Nsid {
        &self.suffix
    }
}
pub type AllTimeRollupVal = CountsValue;

#[derive(Debug, Copy, Clone, PartialEq, Hash, PartialOrd, Eq)]
pub struct TruncatedCursor<const MOD: u64>(u64);
impl<const MOD: u64> TruncatedCursor<MOD> {
    fn truncate(raw: u64) -> u64 {
        (raw / MOD) * MOD
    }
    pub fn try_from_raw_u64(time_us: u64) -> Result<Self, EncodingError> {
        let rem = time_us % MOD;
        if rem != 0 {
            return Err(EncodingError::InvalidTruncated(MOD, rem));
        }
        Ok(Self(time_us))
    }
    pub fn try_from_cursor(cursor: Cursor) -> Result<Self, EncodingError> {
        Self::try_from_raw_u64(cursor.to_raw_u64())
    }
    pub fn truncate_cursor(cursor: Cursor) -> Self {
        let raw = cursor.to_raw_u64();
        let truncated = Self::truncate(raw);
        Self(truncated)
    }
}
impl<const MOD: u64> From<TruncatedCursor<MOD>> for Cursor {
    fn from(truncated: TruncatedCursor<MOD>) -> Self {
        Cursor::from_raw_u64(truncated.0)
    }
}
impl<const MOD: u64> From<Cursor> for TruncatedCursor<MOD> {
    fn from(cursor: Cursor) -> Self {
        Self::truncate_cursor(cursor)
    }
}
impl<const MOD: u64> DbBytes for TruncatedCursor<MOD> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let as_cursor: Cursor = (*self).into();
        as_cursor.to_db_bytes()
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (cursor, n) = Cursor::from_db_bytes(bytes)?;
        let me = Self::try_from_cursor(cursor)?;
        Ok((me, n))
    }
}

const HOUR_IN_MICROS: u64 = 1_000_000 * 3600;
pub type HourTruncatedCursor = TruncatedCursor<HOUR_IN_MICROS>;

const WEEK_IN_MICROS: u64 = HOUR_IN_MICROS * 24 * 7;
pub type WeekTruncatedCursor = TruncatedCursor<WEEK_IN_MICROS>;

#[cfg(test)]
mod test {
    use super::{
        CardinalityEstimator, CountsValue, Cursor, Did, EncodingError, HourTruncatedCursor,
        HourlyRollupKey, Nsid, HOUR_IN_MICROS,
    };
    use crate::db_types::DbBytes;

    #[test]
    fn test_by_hourly_rollup_key() -> Result<(), EncodingError> {
        let nsid = Nsid::new("ab.cd.efg".to_string()).unwrap();
        let original = HourlyRollupKey::new(Cursor::from_raw_u64(4567890).into(), &nsid);
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = HourlyRollupKey::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        let serialized_prefix = original.to_prefix_db_bytes()?;
        assert!(serialized_prefix.starts_with("hourly_counts".as_bytes()));
        assert!(serialized_prefix.starts_with(&serialized_prefix));

        Ok(())
    }

    #[test]
    fn test_by_hourly_rollup_value() -> Result<(), EncodingError> {
        let mut estimator = CardinalityEstimator::new();
        for i in 0..10 {
            estimator.insert(&Did::new(format!("did:plc:inze6wrmsm7pjl7yta3oig7{i}")).unwrap());
        }
        let original = CountsValue::new(123, estimator.clone());
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = CountsValue::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        for i in 10..10_000 {
            estimator.insert(&Did::new(format!("did:plc:inze6wrmsm7pjl7yta3oig{i}")).unwrap());
        }
        let original = CountsValue::new(123, estimator);
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = CountsValue::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        Ok(())
    }

    #[test]
    fn test_hour_truncated_cursor() {
        let us = Cursor::from_raw_u64(1_743_778_483_483_895);
        let hr = HourTruncatedCursor::truncate_cursor(us);
        let back: Cursor = hr.into();
        assert!(back < us);
        let diff = us.to_raw_u64() - back.to_raw_u64();
        assert!(diff < HOUR_IN_MICROS);
    }

    #[test]
    fn test_hour_truncated_cursor_already_truncated() {
        let us = Cursor::from_raw_u64(1_743_775_200_000_000);
        let hr = HourTruncatedCursor::truncate_cursor(us);
        let back: Cursor = hr.into();
        assert_eq!(back, us);
        let diff = us.to_raw_u64() - back.to_raw_u64();
        assert_eq!(diff, 0);
    }
}
