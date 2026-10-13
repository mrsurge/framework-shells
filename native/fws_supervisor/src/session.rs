//! Versioned mutual challenge proofs. Secrets never travel over the socket.
use anyhow::{Result, anyhow, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use tokio::{
    net::UnixStream,
    time::{Duration, timeout},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub type Wire = Framed<UnixStream, LengthDelimitedCodec>;
pub const VERSION: u32 = 1;
pub const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    pub version: u32,
    pub instance: String,
    pub nonce: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proof {
    pub version: u32,
    pub instance: String,
    pub nonce: String,
    pub proof: String,
}

pub fn nonce() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("random source failed: {e}"))?;
    Ok(B64.encode(bytes))
}

fn mac(
    secret: &[u8],
    role: &str,
    challenge: &Challenge,
    client_nonce: &str,
) -> Result<Hmac<Sha256>> {
    ensure!(!secret.is_empty(), "empty secret");
    ensure!(
        challenge.version == VERSION,
        "unsupported supervisor version"
    );
    ensure!(
        B64.decode(&challenge.nonce)?.len() == 32,
        "invalid challenge"
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)?;
    ensure!(
        B64.decode(client_nonce)?.len() == 32,
        "invalid client nonce"
    );
    mac.update(b"fws-supervisor-v1\0");
    mac.update(role.as_bytes());
    mac.update(b"\0");
    mac.update(challenge.instance.as_bytes());
    mac.update(b"\0");
    mac.update(challenge.nonce.as_bytes());
    mac.update(b"\0");
    mac.update(client_nonce.as_bytes());
    Ok(mac)
}

fn make_proof(
    secret: &[u8],
    role: &str,
    challenge: &Challenge,
    client_nonce: &str,
) -> Result<Proof> {
    Ok(Proof {
        version: VERSION,
        instance: challenge.instance.clone(),
        nonce: client_nonce.into(),
        proof: B64.encode(
            mac(secret, role, challenge, client_nonce)?
                .finalize()
                .into_bytes(),
        ),
    })
}

fn verify(secret: &[u8], role: &str, challenge: &Challenge, proof: &Proof) -> Result<()> {
    ensure!(
        proof.version == VERSION && proof.instance == challenge.instance,
        "session identity mismatch"
    );
    mac(secret, role, challenge, &proof.nonce)?.verify_slice(&B64.decode(&proof.proof)?)?;
    Ok(())
}

pub async fn send<T: Serialize>(wire: &mut Wire, value: &T) -> Result<()> {
    timeout(DEADLINE, wire.send(Bytes::from(serde_json::to_vec(value)?))).await??;
    Ok(())
}
pub async fn receive<T: DeserializeOwned>(wire: &mut Wire) -> Result<T> {
    let bytes = wire
        .next()
        .await
        .ok_or_else(|| anyhow!("transport disconnected"))??;
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn authenticate_server(wire: &mut Wire, secret: &[u8], instance: &str) -> Result<()> {
    timeout(DEADLINE, async {
        let challenge = Challenge {
            version: VERSION,
            instance: instance.to_owned(),
            nonce: nonce()?,
        };
        send(wire, &challenge).await?;
        let proof: Proof = receive(wire).await?;
        verify(secret, "client", &challenge, &proof)?;
        send(
            wire,
            &make_proof(secret, "server", &challenge, &proof.nonce)?,
        )
        .await
    })
    .await?
}

pub async fn authenticate_client(wire: &mut Wire, secret: &[u8], instance: &str) -> Result<()> {
    timeout(DEADLINE, async {
        let challenge: Challenge = receive(wire).await?;
        ensure!(challenge.instance == instance, "wrong supervisor instance");
        let client_nonce = nonce()?;
        send(
            wire,
            &make_proof(secret, "client", &challenge, &client_nonce)?,
        )
        .await?;
        let proof: Proof = receive(wire).await?;
        ensure!(
            proof.nonce == client_nonce,
            "server proof for wrong client nonce"
        );
        verify(secret, "server", &challenge, &proof)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proof_is_bound_to_secret_instance_role_and_challenge() {
        let c = Challenge {
            version: VERSION,
            instance: "instance".into(),
            nonce: nonce().unwrap(),
        };
        let p = make_proof(b"secret", "client", &c, &nonce().unwrap()).unwrap();
        assert!(verify(b"secret", "client", &c, &p).is_ok());
        assert!(verify(b"wrong", "client", &c, &p).is_err());
        assert!(verify(b"secret", "server", &c, &p).is_err());
        let other = Challenge {
            nonce: nonce().unwrap(),
            ..c
        };
        assert!(verify(b"secret", "client", &other, &p).is_err());
    }
}
