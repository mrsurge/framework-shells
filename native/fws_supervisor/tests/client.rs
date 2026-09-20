use fws_supervisor::{
    client::Client,
    framed,
    session::{authenticate_server, receive, send},
};
use serde_json::{Value, json};
use tokio::{
    net::UnixListener,
    time::{Duration, timeout},
};

#[tokio::test]
async fn lost_ack_is_uncertain_and_never_retried() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let mut wire = framed(listener.accept().await.unwrap().0);
        authenticate_server(&mut wire, b"secret", "instance")
            .await
            .unwrap();
        send(
            &mut wire,
            &json!({"event":"attached","instance":"instance"}),
        )
        .await
        .unwrap();
        assert_eq!(receive::<Value>(&mut wire).await.unwrap()["op"], "acquire");
        send(&mut wire, &json!({"ok":true,"result":{"generation":1}}))
            .await
            .unwrap();
        assert_eq!(receive::<Value>(&mut wire).await.unwrap()["op"], "write");
        // Drop with no ACK, as if the write happened just before a disconnect.
    });
    timeout(Duration::from_secs(10), async {
        let client = Client::connect(&path, b"secret", "instance").await.unwrap();
        client.acquire(false).await.unwrap();
        assert_eq!(
            client.write(b"one").await.unwrap_err().kind,
            "delivery_uncertain"
        );
        assert!(client.write(b"never-retry").await.is_err());
        client.close().await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumer_overflow_is_explicit_not_silent_loss() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let mut wire = framed(listener.accept().await.unwrap().0);
        authenticate_server(&mut wire, b"secret", "instance")
            .await
            .unwrap();
        send(
            &mut wire,
            &json!({"event":"attached","instance":"instance"}),
        )
        .await
        .unwrap();
        for offset in 0..300 {
            if send(
                &mut wire,
                &json!({"event":"output","stream":"stdout","offset":offset,"data":"eA=="}),
            )
            .await
            .is_err()
            {
                break;
            }
        }
        while receive::<Value>(&mut wire).await.is_ok() {}
    });
    timeout(Duration::from_secs(10), async {
        let client = Client::connect(&path, b"secret", "instance").await.unwrap();
        // Status is a control barrier behind received output, not an arbitrary sleep.
        assert_eq!(client.status().await.unwrap_err().kind, "lagged");
        assert_eq!(client.read().await.unwrap_err().kind, "lagged");
        client.close().await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}
