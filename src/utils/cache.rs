use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{LazyLock, Mutex};

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::AppError;

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("data");

static DBS: LazyLock<Mutex<HashMap<String, Arc<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Cache root override (output directory). Set once at startup via
/// [`set_base_dir`]; falls back to the user home when unset (tests).
static BASE: LazyLock<Mutex<Option<PathBuf>>> = LazyLock::new(|| Mutex::new(None));

/// Point the cache at `output_dir/.cache`. Call once before any read/write.
pub fn set_base_dir(dir: &std::path::Path) -> Result<(), AppError> {
    BASE.lock()
        .map(|mut base| *base = Some(dir.join(".cache")))
        .map_err(|e| AppError::Cache(format!("mutex poisoned: {e}")))
}

fn db_path(domain: &str) -> PathBuf {
    if let Ok(base) = BASE.lock()
        && let Some(root) = base.as_ref()
    {
        return root.join(format!("{domain}/{domain}.redb"));
    }
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(format!(".cache/{domain}/{domain}.redb")),
        None => PathBuf::from(format!("/tmp/{domain}/{domain}.redb")),
    }
}

fn ensure_db(domain: &str) -> Result<Arc<Database>, AppError> {
    let path = db_path(domain);
    let key = path.to_string_lossy().into_owned();
    let mut map = DBS
        .lock()
        .map_err(|e| AppError::Cache(format!("mutex poisoned: {e}")))?;
    if let Some(db) = map.get(&key) {
        return Ok(Arc::clone(db));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(AppError::Io)?;
    }
    let db = Database::create(&path).map_err(|e| AppError::Cache(e.to_string()))?;
    let db = Arc::new(db);
    map.insert(key, Arc::clone(&db));
    Ok(db)
}

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

pub fn read(domain: &str, key: &str, ttl_secs: Option<u64>) -> Option<Vec<u8>> {
    let db = match ensure_db(domain) {
        Ok(db) => db,
        Err(e) => {
            tracing::warn!(domain, "cache read ensure_db failed: {e}");
            return None;
        }
    };
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

pub fn write(domain: &str, key: &str, data: &[u8]) -> Result<(), AppError> {
    let db = ensure_db(domain)?;
    let cached_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&cached_at.to_le_bytes());
    buf.extend_from_slice(data);

    let write_tx = db
        .begin_write()
        .map_err(|e| AppError::Cache(e.to_string()))?;
    {
        let mut table = write_tx
            .open_table(TABLE)
            .map_err(|e| AppError::Cache(e.to_string()))?;
        table
            .insert(key, buf)
            .map_err(|e| AppError::Cache(e.to_string()))?;
    }
    write_tx
        .commit()
        .map_err(|e| AppError::Cache(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Run tests in serial and redirect cache to a temp dir.
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
            write("test_domain", "key1", b"hello").unwrap();
            let val = read("test_domain", "key1", None);
            assert_eq!(val, Some(b"hello".to_vec()));
        });
    }

    #[test]
    fn read_missing_key() {
        with_cache(|| {
            let val = read("test_domain", "nonexistent", None);
            assert_eq!(val, None);
        });
    }

    #[test]
    fn read_expired_entry() {
        with_cache(|| {
            write("test_domain", "key1", b"data").unwrap();
            // TTL = 0 → immediately expired
            let val = read("test_domain", "key1", Some(0));
            assert_eq!(val, None);
        });
    }
}
