//! Frozen v0 objectKV application boundary exercised by the Chess example.

use okv_model::Version;

pub const API_VERSION: &str = "objectkv-boundary-v0";
pub const TX_ENVELOPE_MAGIC: &[u8; 8] = b"OKVTXV00";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvMutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Clear { key: Vec<u8> },
    ClearRange { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactRequest {
    pub tenant: String,
    pub read_version: Version,
    pub request_id: u64,
    pub mutations: Vec<KvMutation>,
    pub application_record: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitReceipt {
    pub api_version: &'static str,
    pub commit_version: Version,
    pub request_id: u64,
    pub replayed: bool,
    pub mutation_count: usize,
    pub txlog_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointReadRequest {
    pub tenant: String,
    pub key: Vec<u8>,
    pub read_version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeReadRequest {
    pub tenant: String,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub read_version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEnvelope {
    pub commit_version: Version,
    pub request_id: u64,
    pub mutations: Vec<KvMutation>,
    pub application_record: Vec<u8>,
}

impl CommittedEnvelope {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TX_ENVELOPE_MAGIC);
        bytes.extend_from_slice(&self.commit_version.to_be_bytes());
        bytes.extend_from_slice(&self.request_id.to_be_bytes());
        put_bytes(&mut bytes, &self.application_record);
        put_u32(&mut bytes, self.mutations.len());
        for mutation in &self.mutations {
            match mutation {
                KvMutation::Set { key, value } => {
                    bytes.push(1);
                    put_bytes(&mut bytes, key);
                    put_bytes(&mut bytes, value);
                }
                KvMutation::Clear { key } => {
                    bytes.push(2);
                    put_bytes(&mut bytes, key);
                }
                KvMutation::ClearRange { start, end } => {
                    bytes.push(3);
                    put_bytes(&mut bytes, start);
                    put_bytes(&mut bytes, end);
                }
            }
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut decoder = Decoder::new(bytes);
        if decoder.take(TX_ENVELOPE_MAGIC.len())? != TX_ENVELOPE_MAGIC {
            return Err("unsupported transaction envelope".to_owned());
        }
        let mut version = [0_u8; 16];
        version.copy_from_slice(decoder.take(16)?);
        let request_id = decoder.u64()?;
        let application_record = decoder.bytes()?;
        let mutation_count = decoder.u32()? as usize;
        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            mutations.push(match decoder.byte()? {
                1 => KvMutation::Set {
                    key: decoder.bytes()?,
                    value: decoder.bytes()?,
                },
                2 => KvMutation::Clear {
                    key: decoder.bytes()?,
                },
                3 => KvMutation::ClearRange {
                    start: decoder.bytes()?,
                    end: decoder.bytes()?,
                },
                tag => return Err(format!("unknown mutation tag {tag}")),
            });
        }
        if !decoder.is_empty() {
            return Err("transaction envelope has trailing bytes".to_owned());
        }
        Ok(Self {
            commit_version: Version::from_be_bytes(version),
            request_id,
            mutations,
            application_record,
        })
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) {
    let value = u32::try_from(value).expect("prototype record fits u32");
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(target: &mut Vec<u8>, value: &[u8]) {
    put_u32(target, value.len());
    target.extend_from_slice(value);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or_else(|| "transaction envelope length overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| "truncated transaction envelope".to_owned())?;
        self.cursor = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        let mut value = [0_u8; 4];
        value.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(value))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let mut value = [0_u8; 8];
        value.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(value))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let count = self.u32()? as usize;
        Ok(self.take(count)?.to_vec())
    }

    const fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}
