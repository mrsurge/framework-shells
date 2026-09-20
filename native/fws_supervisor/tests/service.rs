//! A real Rust client controlling an unmodified stdio child through the supervisor.
use anyhow::{anyhow, ensure};
use fws_supervisor::{
    client::{Client, Event},
    framed,
    session::{authenticate_client, receive, send},
};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixStream,
    process::Command,
    runtime::Builder,
    time::{Duration, timeout},
};

#[test]
fn rust_client_reconnects_to_unmodified_cat() {
    Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("s");
        let config = dir.path().join("config.json");
        std::fs::write(&config, serde_json::to_vec(&json!({
            "socket":socket,"stdout_log":dir.path().join("out"),"stderr_log":dir.path().join("err"),"command":["cat"]
        })).unwrap()).unwrap();
        let mut supervisor = Command::new(env!("CARGO_BIN_EXE_fws-supervisor"))
            .arg(config).env("FRAMEWORK_SHELLS_SECRET", "test-secret")
            .stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true).spawn().unwrap();
        let mut child_pid = None;
        let result = timeout(Duration::from_secs(10), async {
            let mut line = String::new();
            BufReader::new(supervisor.stdout.take().unwrap()).read_line(&mut line).await?;
            let ready: Value = serde_json::from_str(&line)?;
            child_pid = ready["pid"].as_i64();
            let instance = ready["instance"].as_str().ok_or_else(|| anyhow!("missing instance"))?;
            for _ in 0..2 {
                let client = Client::connect(&socket, b"test-secret", instance).await?;
                ensure!(Some(i64::from(client.status().await?.pid)) == child_pid, "child restarted");
                client.acquire(true).await?;
                ensure!(client.write(b"\0\xff\n").await? == 3, "short write");
                match client.read().await? {
                    Event::Output { data, stream, .. } => ensure!(data == b"\0\xff\n" && stream == "stdout", "binary changed"),
                    other => anyhow::bail!("unexpected event {other:?}"),
                }
                client.close().await;
                // Drop this transport with stdin still open; the next attachment must work.
            }
            let mut client = framed(UnixStream::connect(&socket).await?);
            authenticate_client(&mut client, b"test-secret", instance).await?;
            let _: Value = receive(&mut client).await?;
            send(&mut client, &json!({"op":"stop"})).await?;
            let stopped: Value = receive(&mut client).await?;
            ensure!(stopped["ok"] == true, "stop failed");
            ensure!(supervisor.wait().await?.success(), "supervisor failed");
            ensure!(!socket.exists(), "endpoint leaked");
            ensure!(std::fs::read(dir.path().join("out"))? == b"\0\xff\n\0\xff\n", "raw log changed");
            Ok::<(), anyhow::Error>(())
        }).await;
        // Only test-owned processes are eligible for failure cleanup.
        if supervisor.try_wait().unwrap().is_none() {
            if let Some(pid) = child_pid { unsafe { libc::kill(-(pid as i32), libc::SIGKILL); } }
            let _ = supervisor.kill().await;
        }
        result.unwrap().unwrap();
    });
}
