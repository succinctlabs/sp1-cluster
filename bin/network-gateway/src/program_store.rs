//! Durable program registry, owned by the gateway.
//!
//! The cluster's `ArtifactClient` is *ephemeral scratch space* — every artifact
//! is ref-counted and the Redis backend additionally has a 4 hour TTL. That's
//! correct for the per-proof inputs/intermediates the cluster pipeline produces
//! and consumes, but it's the wrong tier for programs, which we want to register
//! once at `setup()` time and prove against indefinitely.
//!
//! `ProgramStore` lives outside that lifecycle: bytes are written by
//! `create_program` and stay there until explicitly deleted. Each `request_proof`
//! reads the ELF and re-uploads it under a fresh ephemeral artifact id so the
//! cluster pipeline still sees a normal one-shot artifact.

use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::{fs, io::AsyncWriteExt};

#[async_trait]
pub trait ProgramStore: Send + Sync + 'static {
    async fn put(&self, vk_hash: &[u8], vk: &[u8], elf: &[u8]) -> Result<()>;

    async fn get(&self, vk_hash: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>>;

    async fn exists(&self, vk_hash: &[u8]) -> Result<bool>;
}

/// Filesystem-backed durable program store.
///
/// One file per `vk_hash`, keyed by hex. File layout: 4-byte little-endian
/// `vk_len`, then `vk` bytes, then ELF bytes for the rest of the file. Writes
/// go through a `<file>.tmp` + rename so a crash mid-write can't leave a torn
/// program record visible to readers.
pub struct FilesystemProgramStore {
    dir: PathBuf,
}

impl FilesystemProgramStore {
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create program store dir {}", dir.display()))?;
        Ok(Self { dir })
    }

    fn path(&self, vk_hash: &[u8]) -> PathBuf {
        self.dir.join(hex::encode(vk_hash))
    }
}

#[async_trait]
impl ProgramStore for FilesystemProgramStore {
    async fn put(&self, vk_hash: &[u8], vk: &[u8], elf: &[u8]) -> Result<()> {
        let final_path = self.path(vk_hash);
        let tmp_path = final_path.with_extension("tmp");

        let vk_len = u32::try_from(vk.len()).context("vk too large for u32 length prefix")?;
        let mut f = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("create {}", tmp_path.display()))?;
        f.write_all(&vk_len.to_le_bytes()).await?;
        f.write_all(vk).await?;
        f.write_all(elf).await?;
        f.sync_all().await?;
        drop(f);

        fs::rename(&tmp_path, &final_path)
            .await
            .with_context(|| format!("rename into {}", final_path.display()))?;
        Ok(())
    }

    async fn get(&self, vk_hash: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let path = self.path(vk_hash);
        let bytes = match fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context(format!("read {}", path.display())),
        };
        if bytes.len() < 4 {
            anyhow::bail!("program record {} truncated", path.display());
        }
        let vk_len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if bytes.len() < 4 + vk_len {
            anyhow::bail!("program record {} truncated", path.display());
        }
        let vk = bytes[4..4 + vk_len].to_vec();
        let elf = bytes[4 + vk_len..].to_vec();
        Ok(Some((vk, elf)))
    }

    async fn exists(&self, vk_hash: &[u8]) -> Result<bool> {
        match fs::metadata(self.path(vk_hash)).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// `(vk_bytes, elf_bytes)` keyed by `vk_hash`.
type ProgramMap = std::collections::HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>;

/// In-memory `ProgramStore` — useful for tests and for transient deployments
/// where program durability across restarts isn't required.
pub struct InMemoryProgramStore {
    inner: tokio::sync::Mutex<ProgramMap>,
}

impl InMemoryProgramStore {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Default::default()),
        }
    }
}

impl Default for InMemoryProgramStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProgramStore for InMemoryProgramStore {
    async fn put(&self, vk_hash: &[u8], vk: &[u8], elf: &[u8]) -> Result<()> {
        self.inner
            .lock()
            .await
            .insert(vk_hash.to_vec(), (vk.to_vec(), elf.to_vec()));
        Ok(())
    }

    async fn get(&self, vk_hash: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.inner.lock().await.get(vk_hash).cloned())
    }

    async fn exists(&self, vk_hash: &[u8]) -> Result<bool> {
        Ok(self.inner.lock().await.contains_key(vk_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mti::prelude::{MagicTypeIdExt, V7};

    fn unique_tempdir() -> PathBuf {
        let id = "gwtest".create_type_id::<V7>().to_string();
        std::env::temp_dir().join(id)
    }

    #[tokio::test]
    async fn fs_store_roundtrip() {
        let dir = unique_tempdir();
        let store = FilesystemProgramStore::new(dir.clone()).unwrap();
        let vk_hash = vec![1u8, 2, 3, 4];
        let vk = b"vk-blob".to_vec();
        let elf = b"\x7fELF\x01\x02\x03 program bytes go here".to_vec();

        assert!(!store.exists(&vk_hash).await.unwrap());
        assert!(store.get(&vk_hash).await.unwrap().is_none());

        store.put(&vk_hash, &vk, &elf).await.unwrap();

        assert!(store.exists(&vk_hash).await.unwrap());
        let (got_vk, got_elf) = store.get(&vk_hash).await.unwrap().unwrap();
        assert_eq!(got_vk, vk);
        assert_eq!(got_elf, elf);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fs_store_overwrite_atomic() {
        let dir = unique_tempdir();
        let store = FilesystemProgramStore::new(dir.clone()).unwrap();
        let vk_hash = vec![9u8; 8];

        store.put(&vk_hash, b"old-vk", b"old-elf").await.unwrap();
        store.put(&vk_hash, b"new-vk", b"new-elf").await.unwrap();

        let (vk, elf) = store.get(&vk_hash).await.unwrap().unwrap();
        assert_eq!(vk, b"new-vk");
        assert_eq!(elf, b"new-elf");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn in_memory_store_roundtrip() {
        let store = InMemoryProgramStore::new();
        let vk_hash = vec![0xaa; 4];
        assert!(!store.exists(&vk_hash).await.unwrap());
        store.put(&vk_hash, b"vk", b"elf").await.unwrap();
        let (vk, elf) = store.get(&vk_hash).await.unwrap().unwrap();
        assert_eq!(vk, b"vk");
        assert_eq!(elf, b"elf");
    }
}
