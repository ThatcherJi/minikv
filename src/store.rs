use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

const OP_PUT: u8 = 1;
const OP_DEL: u8 = 2;
const HEADER_LEN: u64 = 4 + 8 + 1 + 4 + 4; // crc(4)+seq(8)+op(1)+klen(4)+vlen(4)

#[derive(Clone, Copy, Debug)]
struct ValuePos {
    offset: u64,
    len: u32,
}

#[derive(Debug, Default)]
struct StoreCounters {
    puts: AtomicU64,
    gets: AtomicU64,
    deletes: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreStats {
    pub keys: usize,
    pub write_offset: u64,
    pub data_file_bytes: u64,
    pub next_seq: u64,
    pub puts: u64,
    pub gets: u64,
    pub deletes: u64,
}

pub struct Store {
    dir: PathBuf,
    data_path: PathBuf,
    writer: BufWriter<File>,
    write_offset: u64,
    index: HashMap<String, ValuePos>,
    next_seq: u64,
    counters: StoreCounters,
}

impl Store {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let data_path = dir.join("data.log");
        let (index, next_seq, end_offset) = Self::replay(&data_path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&data_path)?;

        Ok(Self {
            dir,
            data_path,
            writer: BufWriter::new(file),
            write_offset: end_offset,
            index,
            next_seq,
            counters: StoreCounters::default(),
        })
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> std::io::Result<()> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let value_offset = self.write_offset + HEADER_LEN + key.len() as u64;

        self.append(seq, OP_PUT, key.as_bytes(), value)?;
        self.index.insert(
            key.to_string(),
            ValuePos {
                offset: value_offset,
                len: value.len() as u32,
            },
        );
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        self.counters.gets.fetch_add(1, Ordering::Relaxed);
        self.read_indexed_value(key)
    }

    pub fn delete(&mut self, key: &str) -> std::io::Result<bool> {
        if !self.index.contains_key(key) {
            return Ok(false);
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        self.append(seq, OP_DEL, key.as_bytes(), &[])?;
        self.index.remove(key);
        self.counters.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn keys(&self) -> Vec<String> {
        self.keys_with_prefix(None, None)
    }

    pub fn keys_with_prefix(&self, prefix: Option<&str>, limit: Option<usize>) -> Vec<String> {
        let mut keys: Vec<_> = self
            .index
            .keys()
            .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .cloned()
            .collect();
        keys.sort();
        if let Some(limit) = limit {
            keys.truncate(limit);
        }
        keys
    }

    pub fn data_file_bytes(&self) -> std::io::Result<u64> {
        Ok(fs::metadata(&self.data_path)?.len())
    }

    pub fn stats(&self) -> std::io::Result<StoreStats> {
        Ok(StoreStats {
            keys: self.index.len(),
            write_offset: self.write_offset,
            data_file_bytes: self.data_file_bytes()?,
            next_seq: self.next_seq,
            puts: self.counters.puts.load(Ordering::Relaxed),
            gets: self.counters.gets.load(Ordering::Relaxed),
            deletes: self.counters.deletes.load(Ordering::Relaxed),
        })
    }

    pub fn compact(&mut self) -> std::io::Result<()> {
        self.writer.flush()?;

        let tmp_path = self.dir.join("data.compact");
        let tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = BufWriter::new(tmp);
        let mut new_index = HashMap::new();
        let mut new_offset = 0u64;
        let mut seq = 0u64;

        for key in self.keys() {
            let value = self
                .read_indexed_value(&key)?
                .expect("index and data file are inconsistent");
            let body = build_record_body(seq, OP_PUT, key.as_bytes(), &value);
            let crc = crc32fast::hash(&body);
            w.write_all(&crc.to_le_bytes())?;
            w.write_all(&body)?;

            let value_offset = new_offset + HEADER_LEN + key.len() as u64;
            new_index.insert(
                key,
                ValuePos {
                    offset: value_offset,
                    len: value.len() as u32,
                },
            );
            new_offset += 4 + body.len() as u64;
            seq += 1;
        }

        w.flush()?;
        drop(w);

        fs::rename(&tmp_path, &self.data_path)?;
        self.writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&self.data_path)?,
        );
        self.index = new_index;
        self.write_offset = new_offset;
        self.next_seq = seq;
        Ok(())
    }

    fn append(&mut self, seq: u64, op: u8, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let body = build_record_body(seq, op, key, value);
        let crc = crc32fast::hash(&body);

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&body)?;
        self.writer.flush()?;
        self.write_offset += 4 + body.len() as u64;
        Ok(())
    }

    fn read_indexed_value(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        let pos = match self.index.get(key) {
            Some(p) => *p,
            None => return Ok(None),
        };

        let mut f = File::open(&self.data_path)?;
        f.seek(SeekFrom::Start(pos.offset))?;
        let mut buf = vec![0u8; pos.len as usize];
        f.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    fn replay(path: &Path) -> std::io::Result<(HashMap<String, ValuePos>, u64, u64)> {
        let mut index = HashMap::new();
        let mut next_seq = 0u64;
        let mut offset = 0u64;
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((index, 0, 0)),
            Err(e) => return Err(e),
        };
        let mut r = BufReader::new(file);

        loop {
            let mut crc_buf = [0u8; 4];
            match r.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let mut hdr = [0u8; 17];
            if r.read_exact(&mut hdr).is_err() {
                break;
            }

            let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let op = hdr[8];
            let klen = u32::from_le_bytes(hdr[9..13].try_into().unwrap()) as usize;
            let vlen = u32::from_le_bytes(hdr[13..17].try_into().unwrap()) as usize;
            let mut kv = vec![0u8; klen + vlen];
            if r.read_exact(&mut kv).is_err() {
                break;
            }

            let mut full = Vec::with_capacity(17 + kv.len());
            full.extend_from_slice(&hdr);
            full.extend_from_slice(&kv);
            if crc32fast::hash(&full) != u32::from_le_bytes(crc_buf) {
                break;
            }

            let key = String::from_utf8_lossy(&kv[..klen]).into_owned();
            let record_len = HEADER_LEN + klen as u64 + vlen as u64;
            match op {
                OP_PUT => {
                    let value_offset = offset + HEADER_LEN + klen as u64;
                    index.insert(
                        key,
                        ValuePos {
                            offset: value_offset,
                            len: vlen as u32,
                        },
                    );
                }
                OP_DEL => {
                    index.remove(&key);
                }
                _ => break,
            }
            next_seq = next_seq.max(seq + 1);
            offset += record_len;
        }

        Ok((index, next_seq, offset))
    }
}

fn build_record_body(seq: u64, op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(17 + key.len() + value.len());
    body.extend_from_slice(&seq.to_le_bytes());
    body.push(op);
    body.extend_from_slice(&(key.len() as u32).to_le_bytes());
    body.extend_from_slice(&(value.len() as u32).to_le_bytes());
    body.extend_from_slice(key);
    body.extend_from_slice(value);
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = Store::open(dir.path()).unwrap();
            s.put("a", b"1").unwrap();
            s.put("b", b"hello").unwrap();
            s.put("a", b"2").unwrap();
            assert_eq!(s.get("a").unwrap().as_deref(), Some(&b"2"[..]));
            assert!(s.delete("b").unwrap());
            assert_eq!(s.get("b").unwrap(), None);
        }

        let mut s = Store::open(dir.path()).unwrap();
        assert_eq!(s.get("a").unwrap().as_deref(), Some(&b"2"[..]));
        assert_eq!(s.get("b").unwrap(), None);
        assert_eq!(s.len(), 1);
        s.compact().unwrap();
        assert_eq!(s.get("a").unwrap().as_deref(), Some(&b"2"[..]));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn stats_track_operations_and_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();

        let initial = s.stats().unwrap();
        assert_eq!(initial.keys, 0);
        assert_eq!(initial.puts, 0);
        assert_eq!(initial.gets, 0);
        assert_eq!(initial.deletes, 0);

        s.put("alpha", b"one").unwrap();
        s.put("beta", b"two").unwrap();
        assert_eq!(s.get("alpha").unwrap().as_deref(), Some(&b"one"[..]));
        assert!(s.delete("beta").unwrap());

        let stats = s.stats().unwrap();
        assert_eq!(stats.keys, 1);
        assert_eq!(stats.puts, 2);
        assert_eq!(stats.gets, 1);
        assert_eq!(stats.deletes, 1);
        assert_eq!(stats.next_seq, 3);
        assert!(stats.write_offset > 0);
        assert_eq!(stats.write_offset, stats.data_file_bytes);
    }

    #[test]
    fn keys_are_sorted_for_stable_admin_output() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();

        s.put("zulu", b"z").unwrap();
        s.put("alpha", b"a").unwrap();
        s.put("middle", b"m").unwrap();
        s.delete("zulu").unwrap();

        assert_eq!(s.keys(), vec!["alpha".to_string(), "middle".to_string()]);
    }

    #[test]
    fn compact_rewrites_live_values_and_updates_stats() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();

        for value in ["one", "two", "three", "four"] {
            s.put("same-key", value.as_bytes()).unwrap();
        }
        s.put("keep", b"value").unwrap();
        s.delete("keep").unwrap();

        let before = s.stats().unwrap().data_file_bytes;
        s.compact().unwrap();
        let after = s.stats().unwrap();

        assert!(after.data_file_bytes < before);
        assert_eq!(after.keys, 1);
        assert_eq!(after.next_seq, 1);
        assert_eq!(s.get("same-key").unwrap().as_deref(), Some(&b"four"[..]));
    }

    #[test]
    fn keys_with_prefix_and_limit_stays_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();

        for key in ["app:3", "sys:1", "app:1", "app:2", "sys:2"] {
            s.put(key, key.as_bytes()).unwrap();
        }

        assert_eq!(
            s.keys_with_prefix(Some("app:"), Some(2)),
            vec!["app:1".to_string(), "app:2".to_string()]
        );
        assert_eq!(
            s.keys_with_prefix(Some("missing:"), None),
            Vec::<String>::new()
        );
    }

    #[test]
    fn delete_missing_key_does_not_increment_delete_counter() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path()).unwrap();

        assert!(!s.delete("missing").unwrap());
        assert_eq!(s.stats().unwrap().deletes, 0);
    }

    #[test]
    fn replay_restores_index_without_process_local_counters() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = Store::open(dir.path()).unwrap();
            s.put("persisted", b"value").unwrap();
            assert_eq!(s.get("persisted").unwrap().as_deref(), Some(&b"value"[..]));
        }

        let s = Store::open(dir.path()).unwrap();
        let stats = s.stats().unwrap();
        assert_eq!(stats.keys, 1);
        assert_eq!(stats.puts, 0);
        assert_eq!(stats.gets, 0);
        assert_eq!(stats.next_seq, 1);
        assert_eq!(s.get("persisted").unwrap().as_deref(), Some(&b"value"[..]));
    }
}
