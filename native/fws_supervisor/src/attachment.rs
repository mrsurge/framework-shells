//! Signed discovery records are hints, never proof of liveness or signal authority.
use crate::{
    client::{Client, Status},
    replay::Cursor,
};
use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub const MAX_RECORD_BYTES: usize = 64 * 1024;
const DOMAIN: &[u8] = b"fws-supervisor-attachment-v1\0";

pub fn record_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_os_string();
    name.push(".attachment.json");
    PathBuf::from(name)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShellIdentity {
    pub id: String,
    pub label: Option<String>,
    pub spec_id: Option<String>,
    pub subgroups: Vec<String>,
}
impl ShellIdentity {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.id.is_empty() && self.id.len() <= 256 && !self.id.contains(['/', '\\', '\0']),
            "invalid shell id"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub version: u32,
    pub runtime_id: String,
    pub shell: ShellIdentity,
    pub instance: String,
    pub pid: u32,
    pub supervisor_pid: u32,
    pub socket: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub command: Vec<String>,
    pub cwd: PathBuf,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed {
    body: String,
    proof: String,
}

pub fn runtime_id(secret: &[u8]) -> String {
    format!("{:x}", Sha256::digest(secret))[..16].to_owned()
}
fn mac(secret: &[u8], body: &[u8]) -> Result<Hmac<Sha256>> {
    ensure!(!secret.is_empty(), "empty secret");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)?;
    mac.update(DOMAIN);
    mac.update(body);
    Ok(mac)
}
fn private_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("missing private directory"))?;
    let meta = fs::symlink_metadata(parent)?;
    ensure!(
        meta.is_dir() && meta.mode() & 0o077 == 0 && meta.uid() == unsafe { libc::geteuid() },
        "attachment directory must be private and owned"
    );
    Ok(())
}
impl Descriptor {
    pub fn validate(&self, secret: &[u8]) -> Result<()> {
        self.shell.validate()?;
        ensure!(self.version == 1, "unsupported attachment version");
        ensure!(
            self.runtime_id == runtime_id(secret),
            "wrong attachment runtime"
        );
        ensure!(
            self.pid > 0 && self.supervisor_pid > 0 && !self.instance.is_empty(),
            "invalid instance identity"
        );
        ensure!(!self.command.is_empty(), "missing command");
        for path in [&self.socket, &self.stdout_log, &self.stderr_log, &self.cwd] {
            ensure!(path.is_absolute(), "attachment paths must be absolute");
        }
        Ok(())
    }
    pub fn encode(&self, secret: &[u8]) -> Result<Vec<u8>> {
        self.validate(secret)?;
        let body = serde_json::to_string(self)?;
        let proof = B64.encode(mac(secret, body.as_bytes())?.finalize().into_bytes());
        let bytes = serde_json::to_vec(&Signed { body, proof })?;
        ensure!(
            bytes.len() <= MAX_RECORD_BYTES,
            "attachment record too large"
        );
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], secret: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_RECORD_BYTES,
            "attachment record too large"
        );
        let signed: Signed = serde_json::from_slice(bytes)?;
        mac(secret, signed.body.as_bytes())?.verify_slice(&B64.decode(signed.proof)?)?;
        let record: Self = serde_json::from_str(&signed.body)?;
        record.validate(secret)?;
        Ok(record)
    }
    pub fn load(path: &Path, secret: &[u8]) -> Result<Self> {
        private_parent(path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let meta = file.metadata()?;
        ensure!(
            meta.is_file() && meta.mode() & 0o077 == 0 && meta.uid() == unsafe { libc::geteuid() },
            "attachment must be a private owned regular file"
        );
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take((MAX_RECORD_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        let record = Self::decode(&bytes, secret)?;
        ensure!(
            record.socket.parent() == path.parent(),
            "attachment/socket directory mismatch"
        );
        Ok(record)
    }
    /// Publish a complete signed record without replacing any prior instance.
    pub fn publish(&self, path: &Path, secret: &[u8]) -> Result<()> {
        private_parent(path)?;
        ensure!(
            self.socket.parent() == path.parent(),
            "attachment/socket directory mismatch"
        );
        let bytes = self.encode(secret)?;
        let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
        file.write_all(&bytes)?;
        file.as_file().sync_all()?;
        file.persist_noclobber(path)?;
        Ok(())
    }
}

pub struct Attachment {
    pub descriptor: Descriptor,
    pub client: Client,
    /// Authenticated state at attachment time, not a permanently-live assertion.
    pub status: Status,
}

/// Observe an existing instance. Never spawn, steal its writer, or signal a PID.
pub async fn attach(
    path: impl AsRef<Path>,
    secret: &[u8],
    expected_shell_id: &str,
    cursor: Option<Cursor>,
) -> Result<Attachment> {
    let (path, key) = (path.as_ref().to_owned(), secret.to_vec());
    let descriptor = tokio::task::spawn_blocking(move || Descriptor::load(&path, &key)).await??;
    ensure!(
        descriptor.shell.id == expected_shell_id,
        "wrong attachment shell id"
    );
    let client = match cursor {
        Some(cursor) => {
            Client::connect_from(&descriptor.socket, secret, &descriptor.instance, cursor).await?
        }
        None => Client::connect(&descriptor.socket, secret, &descriptor.instance).await?,
    };
    let status = client.status().await;
    match status {
        Ok(status) if status.pid == descriptor.pid && status.instance == descriptor.instance => {
            Ok(Attachment {
                descriptor,
                client,
                status,
            })
        }
        other => {
            client.close().await;
            anyhow::bail!("attachment status mismatch or unavailable: {other:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn example(root: &Path) -> Descriptor {
        Descriptor {
            version: 1,
            runtime_id: runtime_id(b"test-secret"),
            shell: ShellIdentity {
                id: "fs_test".into(),
                label: None,
                spec_id: None,
                subgroups: vec![],
            },
            instance: "instance-test".into(),
            pid: 42,
            supervisor_pid: 40,
            socket: root.join("s"),
            stdout_log: root.join("out"),
            stderr_log: root.join("err"),
            command: vec!["cat".into()],
            cwd: root.to_owned(),
        }
    }
    #[test]
    fn shared_descriptor_cases_and_authentication() {
        let root = tempfile::tempdir().unwrap();
        let original = serde_json::to_value(example(root.path())).unwrap();
        #[derive(Deserialize)]
        struct Case {
            field: String,
            value: serde_json::Value,
            valid: bool,
        }
        let cases: Vec<Case> =
            serde_json::from_str(include_str!("../tests/attachment_cases.json")).unwrap();
        for case in cases {
            let mut value = original.clone();
            value[&case.field] = case.value;
            let body = serde_json::to_string(&value).unwrap();
            let proof = B64.encode(
                mac(b"test-secret", body.as_bytes())
                    .unwrap()
                    .finalize()
                    .into_bytes(),
            );
            let bytes = serde_json::to_vec(&Signed { body, proof }).unwrap();
            assert_eq!(
                Descriptor::decode(&bytes, b"test-secret").is_ok(),
                case.valid,
                "{}",
                case.field
            );
            assert!(Descriptor::decode(&bytes, b"other-secret").is_err());
        }
    }
    #[test]
    fn private_bounded_atomic_records_never_replace_an_instance() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("s.attachment.json");
        let descriptor = example(root.path());
        descriptor.publish(&path, b"test-secret").unwrap();
        let original = fs::read(&path).unwrap();
        assert!(descriptor.publish(&path, b"test-secret").is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(
            Descriptor::load(&path, b"test-secret").unwrap().shell,
            descriptor.shell
        );
        let link = root.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(Descriptor::load(&link, b"test-secret").is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Descriptor::load(&path, b"test-secret").is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, vec![b'x'; MAX_RECORD_BYTES + 1]).unwrap();
        assert!(Descriptor::load(&path, b"test-secret").is_err());
    }
}
