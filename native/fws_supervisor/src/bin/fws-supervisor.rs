//! Experimental standalone launch; deliberately not installed by FWS yet.
use anyhow::{Result, ensure};
use fws_supervisor::service::{Config, run};
fn main() -> Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("config path required"))?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let secret = std::env::var("FRAMEWORK_SHELLS_SECRET")?.into_bytes();
    // Detach the supervisor from the launcher's session before starting its child.
    ensure!(
        unsafe { libc::setsid() } != -1,
        "cannot create supervisor session: {}",
        std::io::Error::last_os_error()
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(config, secret))
}
