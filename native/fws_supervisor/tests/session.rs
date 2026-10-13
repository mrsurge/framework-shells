use fws_supervisor::{
    framed,
    session::{authenticate_client, authenticate_server},
};
use tokio::{net::UnixStream, runtime::Builder};

#[test]
fn mutual_authentication_and_rejections_over_uds() {
    Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        for (secret, instance, success) in [(b"secret".as_slice(), "instance", true), (b"wrong".as_slice(), "instance", false), (b"secret".as_slice(), "wrong", false)] {
            let (server, client) = UnixStream::pair().unwrap();
            let (server_result, client_result) = tokio::join!(
                async move { authenticate_server(&mut framed(server), b"secret", "instance").await },
                async move { authenticate_client(&mut framed(client), secret, instance).await },
            );
            assert_eq!(server_result.is_ok(), success);
            assert_eq!(client_result.is_ok(), success);
        }
    });
}
