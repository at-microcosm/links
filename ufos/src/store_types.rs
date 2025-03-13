use crate::db_types::{DbBytes, DbConcat, DbStaticStr, EncodingError, StaticStr, UseBincodePlz};
use crate::{Cursor, Did, Nsid, RecordKey};
use bincode::{Decode, Encode};

#[derive()]
#[derive(Debug, PartialEq)]
pub struct _ByCollectionStaticStr {}
impl StaticStr for _ByCollectionStaticStr {
    fn static_str() -> &'static str {
        "by_collection"
    }
}
type ByCollectionPrefix = DbStaticStr<_ByCollectionStaticStr>;
/// key format: ["by_collection"|collection|js_cursor]
pub type ByCollectionKey = DbConcat<DbConcat<ByCollectionPrefix, Nsid>, Cursor>;
impl ByCollectionKey {
    pub fn new(nsid: Nsid, cursor: Cursor) -> Self {
        Self {
            prefix: DbConcat::from_pair(Default::default(), nsid),
            suffix: cursor,
        }
    }
    pub fn prefix_from_nsid(nsid: Nsid) -> Result<Vec<u8>, EncodingError> {
        DbConcat::from_pair(ByCollectionPrefix::default(), nsid).to_db_bytes()
    }
}
impl From<ByCollectionKey> for (Nsid, Cursor) {
    fn from(k: ByCollectionKey) -> Self {
        (k.prefix.suffix, k.suffix)
    }
}

#[derive(Debug, PartialEq, Encode, Decode)]
pub struct ByCollectionValueInfo {
    #[bincode(with_serde)]
    pub did: Did,
    #[bincode(with_serde)]
    pub rkey: RecordKey,
}
impl UseBincodePlz for ByCollectionValueInfo {}
/// value format: contains did, rkey, record
pub type ByCollectionValue = DbConcat<ByCollectionValueInfo, serde_json::Value>;
impl ByCollectionValue {
    pub fn new(did: Did, rkey: RecordKey, record: serde_json::Value) -> Self {
        Self {
            prefix: ByCollectionValueInfo { did, rkey },
            suffix: record,
        }
    }
}
impl From<ByCollectionValue> for (Did, RecordKey, serde_json::Value) {
    fn from(v: ByCollectionValue) -> Self {
        (v.prefix.did, v.prefix.rkey, v.suffix)
    }
}

#[cfg(test)]
mod test {
    use super::{ByCollectionKey, ByCollectionValue, Cursor, Did, EncodingError, Nsid, RecordKey};
    use crate::db_types::DbBytes;

    #[test]
    fn test_by_collection_key() -> Result<(), EncodingError> {
        let nsid = Nsid::new("ab.cd.efg".to_string()).unwrap();
        let original = ByCollectionKey::new(nsid.clone(), Cursor::from_raw_u64(456));
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ByCollectionKey::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        let serialized_prefix = original.to_prefix_db_bytes()?;
        assert!(serialized.starts_with(&serialized_prefix));
        let just_prefix = ByCollectionKey::prefix_from_nsid(nsid)?;
        assert_eq!(just_prefix, serialized_prefix);
        assert!(just_prefix.starts_with("by_collection".as_bytes()));

        Ok(())
    }

    #[test]
    fn test_by_collection_value() -> Result<(), EncodingError> {
        let did = Did::new("did:plc:inze6wrmsm7pjl7yta3oig77".to_string()).unwrap();
        let rkey = RecordKey::new("asdfasdf".to_string()).unwrap();
        let record = serde_json::Value::String("hellooooo".into());

        let original = ByCollectionValue::new(did, rkey, record);
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ByCollectionValue::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        Ok(())
    }
}
