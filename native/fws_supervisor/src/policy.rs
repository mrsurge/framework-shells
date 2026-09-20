//! Pure persistent-shell policy, not yet wired into launch or signal delivery.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownIntent {
    FrameworkTeardown,
    ExplicitStop,
}

/// Parse an already-rendered flag. Missing fields default to false at callers.
pub fn parse_survival(value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => bail!("survive_shutdown must be a boolean or true/false/1/0 string"),
        },
        _ => bail!("survive_shutdown must be a boolean or true/false/1/0 string"),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShutdownSelection {
    pub terminate: BTreeSet<u32>,
    pub preserved: BTreeSet<u32>,
}

/// Select snapshot PIDs, not signal groups; protected descendants win at teardown.
pub fn select_shutdown(
    parents: &BTreeMap<u32, Option<u32>>,
    roots: &BTreeSet<u32>,
    survival_roots: &BTreeSet<u32>,
    intent: ShutdownIntent,
) -> ShutdownSelection {
    fn descendants(parents: &BTreeMap<u32, Option<u32>>, roots: &BTreeSet<u32>) -> BTreeSet<u32> {
        let mut children = BTreeMap::<u32, Vec<u32>>::new();
        for (pid, parent) in parents {
            if let Some(parent) = parent {
                children.entry(*parent).or_default().push(*pid);
            }
        }
        let mut found = BTreeSet::new();
        let mut pending = roots.iter().copied().collect::<Vec<_>>();
        while let Some(pid) = pending.pop() {
            if found.insert(pid) {
                pending.extend(children.get(&pid).into_iter().flatten().copied());
            }
        }
        found.retain(|pid| parents.contains_key(pid));
        found
    }
    let selected = descendants(parents, roots);
    let protected = match intent {
        ShutdownIntent::FrameworkTeardown => descendants(parents, survival_roots),
        ShutdownIntent::ExplicitStop => BTreeSet::new(),
    };
    ShutdownSelection {
        terminate: selected.difference(&protected).copied().collect(),
        preserved: selected.intersection(&protected).copied().collect(),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterLease {
    pub instance: String,
    pub generation: u64,
    pub connection: String,
}

/// Supervisor-owned state. Serialize transitions and authenticate before calling.
pub struct AttachmentState {
    instance: String,
    generation: u64,
    owner: Option<WriterLease>,
    stdin_closed: bool,
    exited: bool,
}

impl AttachmentState {
    pub fn new(instance: String) -> Result<Self> {
        if instance.is_empty() {
            bail!("instance identity must not be empty");
        }
        Ok(Self {
            instance,
            generation: 0,
            owner: None,
            stdin_closed: false,
            exited: false,
        })
    }
    pub fn acquire(&mut self, connection: String, takeover: bool) -> Result<WriterLease> {
        if connection.is_empty() || self.exited || self.stdin_closed {
            bail!("stdin unavailable");
        }
        if self.owner.is_some() && !takeover {
            bail!("stdin already owned");
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("attachment generation exhausted"))?;
        let lease = WriterLease {
            instance: self.instance.clone(),
            generation: self.generation,
            connection,
        };
        self.owner = Some(lease.clone());
        Ok(lease)
    }
    pub fn can_write(&self, lease: &WriterLease) -> bool {
        !self.exited && !self.stdin_closed && self.owner.as_ref() == Some(lease)
    }
    pub fn detach(&mut self, lease: &WriterLease) {
        if self.owner.as_ref() == Some(lease) {
            self.owner = None;
        }
    }
    pub fn detach_connection(&mut self, connection: &str) {
        if self
            .owner
            .as_ref()
            .is_some_and(|lease| lease.connection == connection)
        {
            self.owner = None;
        }
    }
    pub fn eof(&mut self, lease: &WriterLease) -> Result<()> {
        if !self.can_write(lease) {
            bail!("stale or unavailable writer");
        }
        self.stdin_closed = true;
        self.owner = None;
        Ok(())
    }
    pub fn child_exited(&mut self) {
        self.exited = true;
        self.owner = None;
    }
}
