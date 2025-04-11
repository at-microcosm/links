use std::fmt;
use crate::{Cursor, Did, Nsid, RecordKey};
use bincode::{
    config::{standard, Config},
    de::Decode as BincodeDecode,
    decode_from_slice,
    enc::Encode as BincodeEncode,
    encode_to_vec,
    error::{DecodeError, EncodeError},
};
use lsm_tree::range::prefix_to_range;
use std::marker::PhantomData;
use std::ops::{Bound, Range};
use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum EncodingError {
    #[error("failed to parse Atrium string type: {0}")]
    BadAtriumStringType(&'static str),
    #[error("failed to bincode-encode: {0}")]
    BincodeEncodeFailed(#[from] EncodeError),
    #[error("failed to bincode-decode: {0}")]
    BincodeDecodeFailed(#[from] DecodeError),
    #[error("decode missing suffix bytes")]
    DecodeMissingSuffix,
    #[error("decode ran out of bytes")]
    DecodeNotEnoughBytes,
    #[error("string contained a null byte, which is not allowed, which is annoying, sorry")]
    StringContainedNull,
    #[error("string was not terminated with null byte")]
    UnterminatedString,
    #[error("could not convert from utf8: {0}")]
    NotUtf8(#[from] std::str::Utf8Error),
    #[error("could not convert from utf8: {0}")]
    NotUtf8String(#[from] std::string::FromUtf8Error),
    #[error("could not get array from slice: {0}")]
    BadSlice(#[from] std::array::TryFromSliceError),
    #[error("wrong static prefix. expected {1:?}, found {0:?}")]
    WrongStaticPrefix(String, String), // found, expected
    #[error("failed to deserialize json")]
    JsonError(#[from] serde_json::Error),
    #[error("unexpected extra bytes ({0} bytes) left after decoding")]
    DecodeTooManyBytes(usize),
    #[error("expected exclusive bound from lsm_tree (likely bug)")]
    BadRangeBound,
    #[error("expected a truncated u64 for mod {0}, found remainder: {1}")]
    InvalidTruncated(u64, u64),
}

fn bincode_conf() -> impl Config {
    log::trace!("bincode conf");
    standard()
        .with_big_endian()
        .with_fixed_int_encoding()
        .with_limit::<{ 2_usize.pow(20) }>() // 1MB
}

pub trait DbBytes {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError>;
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized;
}

#[derive(PartialEq)]
pub struct DbConcat<P: DbBytes, S: DbBytes> {
    pub prefix: P,
    pub suffix: S,
}

impl<P: DbBytes + PartialEq + std::fmt::Debug, S: DbBytes + PartialEq + std::fmt::Debug>
    DbConcat<P, S>
{
    pub fn from_pair(prefix: P, suffix: S) -> Self {
        Self { prefix, suffix }
    }
    pub fn from_prefix_to_db_bytes(prefix: &P) -> Result<Vec<u8>, EncodingError> {
        prefix.to_db_bytes()
    }
    pub fn to_prefix_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        self.prefix.to_db_bytes()
    }
    pub fn prefix_range_end(prefix: &P) -> Result<Vec<u8>, EncodingError> {
        let prefix_bytes = prefix.to_db_bytes()?;
        let (_, Bound::Excluded(range_end)) = prefix_to_range(&prefix_bytes) else {
            return Err(EncodingError::BadRangeBound);
        };
        Ok(range_end.to_vec())
    }
    pub fn range_end(&self) -> Result<Vec<u8>, EncodingError> {
        Self::prefix_range_end(&self.prefix)
    }
    pub fn range(&self) -> Result<Range<Vec<u8>>, EncodingError> {
        let prefix_bytes = self.prefix.to_db_bytes()?;
        let (Bound::Included(start), Bound::Excluded(end)) = prefix_to_range(&prefix_bytes) else {
            return Err(EncodingError::BadRangeBound);
        };
        Ok(start.to_vec()..end.to_vec())
    }
    pub fn range_to_prefix_end(&self) -> Result<Range<Vec<u8>>, EncodingError> {
        Ok(self.to_db_bytes()?..self.range_end()?)
    }
}

impl<P: DbBytes + std::fmt::Debug, S: DbBytes + std::fmt::Debug> fmt::Debug for DbConcat<P, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbConcat<{:?} || {:?}>", self.prefix, self.suffix)
    }
}

impl<P: DbBytes, S: DbBytes> DbBytes for DbConcat<P, S> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let mut combined = self.prefix.to_db_bytes()?;
        combined.append(&mut self.suffix.to_db_bytes()?);
        Ok(combined)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError>
    where
        Self: Sized,
    {
        let (prefix, eaten) = P::from_db_bytes(bytes)?;
        let Some(suffix_bytes) = bytes.get(eaten..) else {
            return Err(EncodingError::DecodeMissingSuffix);
        };
        let (suffix, also_eaten) = S::from_db_bytes(suffix_bytes)?;
        Ok((Self { prefix, suffix }, eaten + also_eaten))
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct DbEmpty(());
impl DbBytes for DbEmpty {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(vec![])
    }
    fn from_db_bytes(_: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok((Self(()), 0))
    }
}

pub trait StaticStr {
    fn static_str() -> &'static str;
}

#[derive(PartialEq)]
pub struct DbStaticStr<S: StaticStr> {
    marker: PhantomData<S>,
}
impl<S: StaticStr> Default for DbStaticStr<S> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}
impl<S: StaticStr> fmt::Debug for DbStaticStr<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbStaticStr({:?})", S::static_str())
    }
}
impl<S: StaticStr> DbBytes for DbStaticStr<S> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        S::static_str().to_string().to_db_bytes()
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (prefix, eaten) = String::from_db_bytes(bytes)?;
        if prefix != S::static_str() {
            return Err(EncodingError::WrongStaticPrefix(
                prefix,
                S::static_str().to_string(),
            ));
        }
        Ok((
            Self {
                marker: PhantomData,
            },
            eaten,
        ))
    }
}

/// marker trait: impl on a type to indicate that that DbBytes should use bincode on it
pub trait UseBincodePlz {}

impl<T> DbBytes for T
where
    T: BincodeEncode + BincodeDecode<()> + UseBincodePlz + Sized + std::fmt::Debug,
{
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        log::info!("bincode to_db_bytes: {self:?}");
        Ok(encode_to_vec(self, bincode_conf())?)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode from_db_bytes...");
        Ok(decode_from_slice(bytes, bincode_conf())?)
    }
}

/// helper trait: impl on a type to get helpers to implement DbBytes
pub trait SerdeBytes: serde::Serialize + for<'a> serde::Deserialize<'a> {
    fn to_bytes(&self) -> Result<Vec<u8>, EncodingError> where Self: std::fmt::Debug {
        log::info!("bincode serde to_db_bytes: {self:?}");
        Ok(bincode::serde::encode_to_vec(self, bincode_conf())?)
    }
    fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode serde from_db_bytes...");
        Ok(bincode::serde::decode_from_slice(bytes, bincode_conf())?)
    }
}

//////

impl DbBytes for Vec<u8> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        log::info!("bincode vec to_db_bytes");
        Ok(self.to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode vec from_db_bytes...");
        Ok((bytes.to_owned(), bytes.len()))
    }
}

/// Lexicographic-sort-friendly null-terminating serialization for String
///
/// Null bytes technically can appear within utf-8 strings. Currently we will just bail in that case.
///
/// In the future, null bytes could be escaped, or maybe this becomes SLIP-encoded. Either should be
/// backwards-compatible I think.
///
/// TODO: wrap in another type. it's actually probably not desirable to serialize strings this way
/// *except* where needed as a prefix.
impl DbBytes for String {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        let mut v = self.as_bytes().to_vec();
        if v.contains(&0x00) {
            return Err(EncodingError::StringContainedNull);
        }
        v.push(0x00);
        Ok(v)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == 0x00 {
                let (string_bytes, _) = bytes.split_at(i);
                let s = std::str::from_utf8(string_bytes)?;
                return Ok((s.to_string(), i + 1)); // +1 for the null byte
            }
        }
        Err(EncodingError::UnterminatedString)
    }
}

impl DbBytes for Did {
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode did dbbytes from_db_bytes...");
        let (s, n) = decode_from_slice(bytes, bincode_conf())?;
        let me = Self::new(s).map_err(EncodingError::BadAtriumStringType)?;
        Ok((me, n))
    }
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        log::info!("bincode did dbbytes to_db_bytes {self:?}");
        Ok(encode_to_vec(self.as_ref(), bincode_conf())?)
    }
}

impl DbBytes for Nsid {
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode nsid dbbytes from_db_bytes...");
        let (s, n) = decode_from_slice(bytes, bincode_conf())?;
        let me = Self::new(s).map_err(EncodingError::BadAtriumStringType)?;
        Ok((me, n))
    }
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        log::info!("bincode nsid dbbytes to_db_bytes {self:?}");
        Ok(encode_to_vec(self.as_ref(), bincode_conf())?)
    }
}

impl DbBytes for RecordKey {
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        log::info!("bincode rkey dbbytes from_db_bytes...");
        let (s, n) = decode_from_slice(bytes, bincode_conf())?;
        let me = Self::new(s).map_err(EncodingError::BadAtriumStringType)?;
        Ok((me, n))
    }
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        log::info!("bincode rkey dbbytes to_db_bytes {self:?}");
        Ok(encode_to_vec(self.as_ref(), bincode_conf())?)
    }
}

impl DbBytes for Cursor {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.to_raw_u64().to_be_bytes().to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        if bytes.len() < 8 {
            return Err(EncodingError::DecodeNotEnoughBytes);
        }
        let bytes8 = TryInto::<[u8; 8]>::try_into(&bytes[..8])?;
        let cursor = Cursor::from_raw_u64(u64::from_be_bytes(bytes8));
        Ok((cursor, 8))
    }
}

impl DbBytes for serde_json::Value {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        self.to_string().to_db_bytes()
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (s, n) = String::from_db_bytes(bytes)?;
        let v = s.parse()?;
        Ok((v, n))
    }
}

pub fn db_complete<T: DbBytes>(bytes: &[u8]) -> Result<T, EncodingError> {
    let (t, n) = T::from_db_bytes(bytes)?;
    if n < bytes.len() {
        return Err(EncodingError::DecodeTooManyBytes(bytes.len() - n));
    }
    Ok(t)
}

#[cfg(test)]
mod test {
    use super::{Cursor, DbBytes, DbConcat, DbEmpty, DbStaticStr, EncodingError, StaticStr};

    #[test]
    fn test_db_empty() -> Result<(), EncodingError> {
        let original = DbEmpty::default();
        let serialized = original.to_db_bytes()?;
        assert_eq!(serialized.len(), 0);
        let (restored, bytes_consumed) = DbEmpty::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, 0);
        Ok(())
    }

    #[test]
    fn test_string_roundtrip() -> Result<(), EncodingError> {
        for (case, desc) in [
            ("", "empty string"),
            ("a", "basic string"),
            ("asdf asdf asdf even µnicode", "unicode string"),
        ] {
            let serialized = case.to_string().to_db_bytes()?;
            let (restored, bytes_consumed) = String::from_db_bytes(&serialized)?;
            assert_eq!(&restored, case, "string round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_string_serialized_lexicographic_sort() -> Result<(), EncodingError> {
        let aa = "aa".to_string().to_db_bytes()?;
        let b = "b".to_string().to_db_bytes()?;
        assert!(b > aa);
        Ok(())
    }

    #[test]
    fn test_string_cursor_prefix_roundtrip() -> Result<(), EncodingError> {
        type TwoThings = DbConcat<String, Cursor>;
        for (lazy_prefix, tired_suffix, desc) in [
            ("", 0, "empty string and cursor"),
            ("aaa", 0, "zero-cursor"),
            ("", 1234, "empty string"),
            ("aaaaa", 789, "string and cursor"),
        ] {
            let original = TwoThings {
                prefix: lazy_prefix.to_string(),
                suffix: Cursor::from_raw_u64(tired_suffix),
            };
            let serialized = original.to_db_bytes()?;
            let (restored, bytes_consumed) = TwoThings::from_db_bytes(&serialized)?;
            assert_eq!(restored, original, "round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_cursor_string_prefix_roundtrip() -> Result<(), EncodingError> {
        type TwoThings = DbConcat<Cursor, String>;
        for (tired_prefix, sad_suffix, desc) in [
            (0, "", "empty string and cursor"),
            (0, "aaa", "zero-cursor"),
            (1234, "", "empty string"),
            (789, "aaaaa", "string and cursor"),
        ] {
            eprintln!("{desc}");
            let original = TwoThings {
                prefix: Cursor::from_raw_u64(tired_prefix),
                suffix: sad_suffix.to_string(),
            };
            let serialized = original.to_db_bytes()?;
            let (restored, bytes_consumed) = TwoThings::from_db_bytes(&serialized)?;
            assert_eq!(restored, original, "round-trip: {desc}");
            assert_eq!(
                bytes_consumed,
                serialized.len(),
                "exact bytes consumed for round-trip: {desc}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_static_str() -> Result<(), EncodingError> {
        #[derive(Debug, PartialEq)]
        struct AStaticStr {}
        impl StaticStr for AStaticStr {
            fn static_str() -> &'static str {
                "a static str"
            }
        }
        type ADbStaticStr = DbStaticStr<AStaticStr>;

        let original = ADbStaticStr::default();
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ADbStaticStr::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());
        assert!(serialized.starts_with("a static str".as_bytes()));

        Ok(())
    }

    #[test]
    fn test_static_str_empty() -> Result<(), EncodingError> {
        #[derive(Debug, PartialEq)]
        struct AnEmptyStr {}
        impl StaticStr for AnEmptyStr {
            fn static_str() -> &'static str {
                ""
            }
        }
        type ADbEmptyStr = DbStaticStr<AnEmptyStr>;
        let original = ADbEmptyStr::default();
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ADbEmptyStr::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());
        assert_eq!(serialized, &[0x00]);

        Ok(())
    }

    #[test]
    fn test_static_prefix() -> Result<(), EncodingError> {
        #[derive(Debug, PartialEq)]
        struct AStaticPrefix {}
        impl StaticStr for AStaticPrefix {
            fn static_str() -> &'static str {
                "a static prefix"
            }
        }
        type ADbStaticPrefix = DbStaticStr<AStaticPrefix>;

        type PrefixedCursor = DbConcat<ADbStaticPrefix, Cursor>;

        let original = PrefixedCursor {
            prefix: Default::default(),
            suffix: Cursor::from_raw_u64(123),
        };
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = PrefixedCursor::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());
        assert_eq!(restored.suffix.to_raw_u64(), 123);
        assert!(serialized.starts_with("a static prefix".as_bytes()));

        Ok(())
    }
}
