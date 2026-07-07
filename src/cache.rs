use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{LazyLock, Mutex};

use redb::{Database, TableDefinition};

macro_rules! log_none {
    ($expr:expr, $domain:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: $msg, domain = $domain, error = %e);
                return None;
            }
        }
    };
    ($expr:expr, $domain:expr, $key:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: $msg, domain = $domain, key = $key, error = %e);
                return None;
            }
        }
    };
}

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("data");

static DBS: LazyLock<Mutex<HashMap<String, Arc<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn db_path(domain: &str) -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(format!(".cache/{domain}/{domain}.redb")),
        None => PathBuf::from(format!("/tmp/{domain}/{domain}.redb")),
    }
}

fn ensure_db(domain: &str) -> Option<Arc<Database>> {
    let mut map = DBS.lock().unwrap_or_else(|e| {
        tracing::warn!(domain, "cache mutex poisoned: {e}");
        e.into_inner()
    });
    if let Some(db) = map.get(domain) {
        return Some(Arc::clone(db));
    }
    let path = db_path(domain);
    if let Some(parent) = path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::error!(domain, path = %parent.display(), "cache ensure_db create_dir_all: {e}");
        return None;
    }
    let db = match Database::create(&path) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            tracing::error!(domain, path = %path.display(), "cache ensure_db create: {e}");
            return None;
        }
    };
    map.insert(domain.to_string(), Arc::clone(&db));
    Some(db)
}

pub fn read(domain: &str, key: &str, ttl_secs: Option<u64>) -> Option<Vec<u8>> {
    let db = ensure_db(domain)?;
    let read_tx = log_none!(db.begin_read(), domain, "cache read begin_read");
    let table = log_none!(read_tx.open_table(TABLE), domain, "cache read open_table");
    let guard = log_none!(table.get(key), domain, key, "cache read get")?;
    let raw = guard.value();

    if raw.len() < 8 {
        return None;
    }
    let (ts_bytes, data) = raw.split_at(8);
    let cached_at = u64::from_le_bytes(ts_bytes.try_into().ok()?);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Some(ttl) = ttl_secs
        && cached_at + ttl <= now
    {
        tracing::debug!(domain, key, "cache entry expired");
        return None;
    }
    Some(data.to_vec())
}

pub fn write(domain: &str, key: &str, data: &[u8]) {
    let db = match ensure_db(domain) {
        Some(db) => db,
        None => {
            tracing::warn!(domain, "cache write skipped: ensure_db failed");
            return;
        }
    };
    let cached_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&cached_at.to_le_bytes());
    buf.extend_from_slice(data);
    if let Ok(write_tx) = db.begin_write() {
        if let Ok(mut table) = write_tx.open_table(TABLE) {
            if table.insert(key, buf).is_err() {
                tracing::error!(domain, key, "cache write insert failed");
            }
        } else {
            tracing::error!(domain, key, "cache write open_table failed");
        }
        if write_tx.commit().is_err() {
            tracing::error!(domain, key, "cache write commit failed");
        }
    } else {
        tracing::error!(domain, key, "cache write begin_write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn with_cache(f: impl FnOnce()) {
        static LOCK: Mutex<()> = Mutex::new(());
        let _guard = LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let prev = std::env::var_os("HOME");
        unsafe { std::env::set_var("HOME", dir.path()) };
        DBS.lock().unwrap().clear();
        f();
        match prev {
            Some(h) => unsafe { std::env::set_var("HOME", h) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        drop(_guard);
    }

    #[test]
    fn write_then_read() {
        with_cache(|| {
            write("test", "key1", b"hello");
            let val = read("test", "key1", None);
            assert_eq!(val, Some(b"hello".to_vec()));
        });
    }

    #[test]
    fn read_missing_key() {
        with_cache(|| {
            let val = read("test", "nonexistent", None);
            assert_eq!(val, None);
        });
    }

    #[test]
    fn read_expired_entry() {
        with_cache(|| {
            write("test", "key1", b"data");
            let val = read("test", "key1", Some(0));
            assert_eq!(val, None);
        });
    }
}
