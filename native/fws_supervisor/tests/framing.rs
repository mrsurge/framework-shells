use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use fws_supervisor::{MAX_FRAME_BYTES, framed};
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    runtime::Builder,
    time::{Duration, timeout},
};

#[test]
fn uds_framing_preserves_binary_and_enforces_bounds() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(5), async {
                let (left, right) = UnixStream::pair().unwrap();
                let mut writer = framed(left);
                let mut reader = framed(right);
                let bytes = Bytes::from_static(b"\0\xff\n\r\x80");
                writer.send(bytes.clone()).await.unwrap();
                assert_eq!(reader.next().await.unwrap().unwrap().freeze(), bytes);
                writer.send(Bytes::new()).await.unwrap();
                assert!(reader.next().await.unwrap().unwrap().is_empty());
                assert!(
                    writer
                        .send(Bytes::from(vec![0; MAX_FRAME_BYTES + 1]))
                        .await
                        .is_err()
                );
            })
            .await
            .unwrap();
        });
}

#[test]
fn uds_rejects_oversized_header_before_payload_and_truncated_frame() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(5), async {
                let (mut left, right) = UnixStream::pair().unwrap();
                let mut reader = framed(right);
                left.write_all(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes())
                    .await
                    .unwrap();
                assert!(reader.next().await.unwrap().is_err());
                let (mut left, right) = UnixStream::pair().unwrap();
                let mut reader = framed(right);
                left.write_all(&[0, 0, 0, 3, 42]).await.unwrap();
                left.shutdown().await.unwrap();
                assert!(reader.next().await.unwrap().is_err());
            })
            .await
            .unwrap();
        });
}

#[test]
fn split_header_and_coalesced_frames() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(5), async {
                let (mut left, right) = UnixStream::pair().unwrap();
                let mut reader = framed(right);
                left.write_all(&[0, 0]).await.unwrap();
                left.write_all(&[0, 2, 0, 255, 0, 0, 0, 1, 10])
                    .await
                    .unwrap();
                left.shutdown().await.unwrap();
                assert_eq!(&reader.next().await.unwrap().unwrap()[..], &[0, 255]);
                assert_eq!(&reader.next().await.unwrap().unwrap()[..], &[10]);
                assert!(reader.next().await.is_none());
            })
            .await
            .unwrap();
        });
}
