//! Codec-independent replay bounds and per-stream continuity. Raw logs are never
//! truncated for replay. The caller must atomically subscribe and snapshot ends.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{os::unix::fs::MetadataExt, path::PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, SeekFrom},
};

pub const MAX_REPLAY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub stdout: u64,
    pub stderr: u64,
}
impl Cursor {
    pub fn get(self, stream: &str) -> Result<u64> {
        match stream {
            "stdout" => Ok(self.stdout),
            "stderr" => Ok(self.stderr),
            _ => anyhow::bail!("invalid stream"),
        }
    }
    pub fn set(&mut self, stream: &str, value: u64) -> Result<()> {
        match stream {
            "stdout" => self.stdout = value,
            "stderr" => self.stderr = value,
            _ => anyhow::bail!("invalid stream"),
        }
        Ok(())
    }
    pub fn replay_bytes(self, end: Self) -> Result<u64> {
        ensure!(
            self.stdout <= end.stdout && self.stderr <= end.stderr,
            "replay cursor beyond published output"
        );
        let count = (end.stdout - self.stdout)
            .checked_add(end.stderr - self.stderr)
            .ok_or_else(|| anyhow::anyhow!("replay span overflow"))?;
        ensure!(
            count <= MAX_REPLAY_BYTES,
            "replay span exceeds 1048576 bytes; explicit bounded history recovery required"
        );
        Ok(count)
    }
    /// Return the already-delivered prefix length; None means a full duplicate.
    /// Future offsets are gaps, never an invitation to silently advance a cursor.
    pub fn accept(&mut self, stream: &str, offset: u64, len: usize) -> Result<Option<usize>> {
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| anyhow::anyhow!("output offset overflow"))?;
        let next = self.get(stream)?;
        ensure!(offset <= next, "output gap after replay");
        if end <= next {
            return Ok(None);
        }
        self.set(stream, end)?;
        Ok(Some((next - offset) as usize))
    }
}

pub struct Source {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Source {
    pub async fn new(path: PathBuf, file: &File) -> Result<Self> {
        let metadata = file.metadata().await?;
        Ok(Self {
            path,
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
    pub async fn open(&self, start: u64, end: u64) -> Result<File> {
        ensure!(start <= end, "invalid replay range");
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.path)
            .await?;
        self.verify(&file, end).await?;
        file.seek(SeekFrom::Start(start)).await?;
        Ok(file)
    }
    pub async fn verify(&self, file: &File, end: u64) -> Result<()> {
        let handle = file.metadata().await?;
        let path = tokio::fs::symlink_metadata(&self.path).await?;
        for metadata in [handle, path] {
            ensure!(
                metadata.is_file() && (metadata.dev(), metadata.ino()) == (self.dev, self.ino),
                "replay source replaced"
            );
            ensure!(metadata.len() >= end, "replay source truncated");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_and_atomic_boundary_filter() {
        let end = Cursor {
            stdout: 10,
            stderr: 5,
        };
        assert_eq!(Cursor::default().replay_bytes(end).unwrap(), 15);
        assert!(
            Cursor {
                stdout: 11,
                stderr: 0
            }
            .replay_bytes(end)
            .is_err()
        );
        assert!(
            Cursor::default()
                .replay_bytes(Cursor {
                    stdout: MAX_REPLAY_BYTES,
                    stderr: 1
                })
                .is_err()
        );
        let mut cursor = end;
        assert_eq!(cursor.accept("stdout", 0, 10).unwrap(), None);
        assert_eq!(cursor.accept("stdout", 8, 5).unwrap(), Some(2));
        assert_eq!(cursor.accept("stdout", 13, 3).unwrap(), Some(0));
        assert!(cursor.accept("stderr", 6, 1).is_err());
        assert!(cursor.accept("stdout", u64::MAX, 2).is_err());
        assert_eq!(
            cursor,
            Cursor {
                stdout: 16,
                stderr: 5
            }
        );
    }
    #[tokio::test]
    async fn sources_reject_replacement_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        tokio::fs::write(&path, b"original").await.unwrap();
        let file = File::open(&path).await.unwrap();
        let source = Source::new(path.clone(), &file).await.unwrap();
        assert!(source.open(0, 8).await.is_ok());
        tokio::fs::write(&path, b"x").await.unwrap();
        assert!(source.open(0, 8).await.is_err());
        tokio::fs::rename(&path, dir.path().join("old"))
            .await
            .unwrap();
        tokio::fs::write(&path, b"replacement").await.unwrap();
        assert!(source.open(0, 1).await.is_err());
    }
}
