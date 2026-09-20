//! Disposable interoperability probe, not a production supervisor client.
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use fws_supervisor::{MAX_FRAME_BYTES, framed};
use tokio::{
    net::UnixStream,
    runtime::Builder,
    time::{Duration, timeout},
};

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("socket path required"))?;
    Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            timeout(Duration::from_secs(5), async {
                let mut stream = framed(UnixStream::connect(path).await?);
                for payload in [
                    Bytes::new(),
                    Bytes::from_static(b"\0\xff\n\x80"),
                    Bytes::from(vec![0xa5; MAX_FRAME_BYTES]),
                ] {
                    stream.send(payload.clone()).await?;
                    let received = stream.next().await.ok_or_else(|| anyhow!("early EOF"))??;
                    ensure!(received.freeze() == payload, "payload mismatch");
                }
                Ok::<(), anyhow::Error>(())
            })
            .await?
        })
}
