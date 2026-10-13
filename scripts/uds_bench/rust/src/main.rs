use anyhow::{Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{io::Write, time::Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

#[allow(warnings)]
mod envelope_capnp {
    include!(concat!(env!("OUT_DIR"), "/envelope_capnp.rs"));
}
#[allow(warnings)]
mod generated_flat {
    include!(concat!(env!("OUT_DIR"), "/envelope_generated.rs"));
}
use generated_flat::fws_bench as fb;

const LIMIT: usize = 65536;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Envelope {
    version: u16,
    generation: u64,
    stream: u8,
    offset: u64,
    payload: Vec<u8>,
}
impl Envelope {
    fn new(payload: Vec<u8>) -> Self {
        Self {
            version: 1,
            generation: 0x123456789abcdef0,
            stream: 1,
            offset: 0xfedcba9876543210,
            payload,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct JsonEnvelope {
    version: u16,
    operation: String,
    generation: u64,
    stream: u8,
    offset: u64,
    payload: String,
}

fn encode(codec: &str, value: &Envelope) -> Result<Vec<u8>> {
    Ok(match codec {
        "raw" => value.payload.clone(),
        "json" | "json-msgspec" => serde_json::to_vec(&JsonEnvelope {
            version: value.version,
            operation: "data".into(),
            generation: value.generation,
            stream: value.stream,
            offset: value.offset,
            payload: STANDARD.encode(&value.payload),
        })?,
        "flatbuffers" => {
            let mut b = flatbuffers::FlatBufferBuilder::with_capacity(value.payload.len() + 128);
            let payload = b.create_vector(&value.payload);
            let root = fb::Envelope::create(
                &mut b,
                &fb::EnvelopeArgs {
                    version: value.version,
                    operation: fb::Operation::Data,
                    generation: value.generation,
                    stream: value.stream,
                    offset: value.offset,
                    payload: Some(payload),
                },
            );
            fb::finish_envelope_buffer(&mut b, root);
            b.finished_data().to_vec()
        }
        "capnp" => {
            let mut message = capnp::message::Builder::new_default();
            let mut root = message.init_root::<envelope_capnp::envelope::Builder>();
            root.set_version(value.version);
            root.set_operation(envelope_capnp::envelope::Operation::Data);
            root.set_generation(value.generation);
            root.set_stream(value.stream);
            root.set_offset(value.offset);
            root.set_payload(&value.payload);
            capnp::serialize::write_message_to_words(&message)
        }
        _ => bail!("unknown codec"),
    })
}

fn decode(codec: &str, bytes: &[u8]) -> Result<Envelope> {
    Ok(match codec {
        "raw" => Envelope::new(bytes.to_vec()),
        "json" | "json-msgspec" => {
            let value: JsonEnvelope = serde_json::from_slice(bytes)?;
            ensure!(value.operation == "data", "invalid operation");
            Envelope {
                version: value.version,
                generation: value.generation,
                stream: value.stream,
                offset: value.offset,
                payload: STANDARD.decode(value.payload)?,
            }
        }
        "flatbuffers" => {
            ensure!(
                bytes.len() >= 8 && fb::envelope_buffer_has_identifier(bytes),
                "wrong identifier"
            );
            let value = fb::root_as_envelope(bytes)?;
            ensure!(
                value.operation() == fb::Operation::Data,
                "invalid operation"
            );
            Envelope {
                version: value.version(),
                generation: value.generation(),
                stream: value.stream(),
                offset: value.offset(),
                payload: value.payload().bytes().to_vec(),
            }
        }
        "capnp" => {
            let mut options = capnp::message::ReaderOptions::new();
            options.traversal_limit_in_words(Some(16384));
            options.nesting_limit(16);
            let message = capnp::serialize::read_message(bytes, options)?;
            let value = message.get_root::<envelope_capnp::envelope::Reader>()?;
            ensure!(
                value.get_operation()? == envelope_capnp::envelope::Operation::Data,
                "invalid operation"
            );
            Envelope {
                version: value.get_version(),
                generation: value.get_generation(),
                stream: value.get_stream(),
                offset: value.get_offset(),
                payload: value.get_payload()?.to_vec(),
            }
        }
        _ => bail!("unknown codec"),
    })
}

async fn read(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let size = stream.read_u32().await? as usize;
    ensure!(size <= LIMIT, "oversized frame");
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}
async fn send(stream: &mut UnixStream, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= LIMIT, "oversized frame");
    let mut frame = Vec::with_capacity(bytes.len() + 4);
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(bytes);
    stream.write_all(&frame).await?;
    Ok(())
}
async fn server(path: &str, codec: String) -> Result<()> {
    let listener = UnixListener::bind(path)?;
    println!("ready");
    std::io::stdout().flush()?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        let codec = codec.clone();
        tokio::spawn(async move {
            loop {
                let input = match read(&mut stream).await {
                    Ok(input) => input,
                    Err(_) => break,
                };
                let value = decode(&codec, &input)?;
                send(&mut stream, &encode(&codec, &value)?).await?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }
}

fn cpu_seconds() -> Result<f64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // The process CPU clock measures this client, not the separate server.
    ensure!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) } == 0,
        "process CPU clock unavailable"
    );
    Ok(ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9)
}

async fn client(
    path: &str,
    codec: String,
    size: usize,
    sessions: usize,
    requests: usize,
) -> Result<()> {
    ensure!(
        sessions > 0 && requests > 0 && size <= 32768,
        "invalid workload"
    );
    let value = Envelope::new((0..size).map(|i| (i % 256) as u8).collect());
    let mut connections = Vec::new();
    for _ in 0..sessions {
        let mut stream = UnixStream::connect(path).await?;
        send(&mut stream, &encode(&codec, &value)?).await?;
        ensure!(
            decode(&codec, &read(&mut stream).await?)? == value,
            "warmup mismatch"
        );
        connections.push(stream);
    }
    let cpu = cpu_seconds()?;
    let start = Instant::now();
    let mut tasks = Vec::new();
    for (i, mut stream) in connections.into_iter().enumerate() {
        let codec = codec.clone();
        let value = value.clone();
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut wire = 0usize;
            for sequence in (i..requests).step_by(sessions) {
                let started = Instant::now();
                let mut expected = value.clone();
                if expected.payload.len() >= 8 {
                    expected.payload[..8].copy_from_slice(&(sequence as u64).to_be_bytes());
                }
                let frame = encode(&codec, &expected)?;
                send(&mut stream, &frame).await?;
                let returned = read(&mut stream).await?;
                ensure!(decode(&codec, &returned)? == expected, "response mismatch");
                latencies.push(started.elapsed().as_secs_f64() * 1000.0);
                wire += frame.len() + returned.len() + 8;
            }
            Ok::<_, anyhow::Error>((latencies, wire))
        }));
    }
    let mut latencies = Vec::new();
    let mut wire = 0;
    for task in tasks {
        let (times, count) = task.await??;
        latencies.extend(times);
        wire += count;
    }
    let seconds = start.elapsed().as_secs_f64();
    let cpu = cpu_seconds()? - cpu;
    latencies.sort_by(f64::total_cmp);
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let stddev =
        (latencies.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / latencies.len() as f64).sqrt();
    println!(
        "{}",
        serde_json::json!({"completed": latencies.len(), "errors": 0,
        "seconds": seconds, "rps": requests as f64 / seconds, "client_cpu_seconds": cpu,
        "useful_bytes_per_second": (requests * size * 2) as f64 / seconds, "wire_bytes": wire,
        "rtt_ms": {"min": percentile(0.0), "p50": percentile(0.5), "p95": percentile(0.95),
        "p99": percentile(0.99), "max": percentile(1.0), "mean": mean, "stddev": stddev}})
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") if args.len() == 4 => server(&args[2], args[3].clone()).await,
        Some("client") if args.len() == 7 => {
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                client(
                    &args[2],
                    args[3].clone(),
                    args[4].parse()?,
                    args[5].parse()?,
                    args[6].parse()?,
                ),
            )
            .await?
        }
        _ => bail!(
            "usage: fws-envelope-bench server PATH CODEC | client PATH CODEC SIZE SESSIONS REQUESTS"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_and_metadata_roundtrip() -> Result<()> {
        for codec in ["raw", "json", "flatbuffers", "capnp"] {
            for size in [0, 1, 256, 32768] {
                let value = Envelope::new((0..size).map(|i| (i % 256) as u8).collect());
                ensure!(decode(codec, &encode(codec, &value)?)? == value);
            }
        }
        Ok(())
    }
    #[test]
    fn generated_envelopes_reject_truncation() -> Result<()> {
        for codec in ["flatbuffers", "capnp"] {
            let value = Envelope::new(vec![7; 256]);
            let mut bytes = encode(codec, &value)?;
            bytes.truncate(bytes.len() / 2);
            ensure!(decode(codec, &bytes).is_err());
        }
        Ok(())
    }
}
