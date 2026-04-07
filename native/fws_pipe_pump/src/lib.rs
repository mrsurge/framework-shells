use libc::{dup, poll, pollfd, POLLERR, POLLHUP, POLLIN, POLLNVAL};
use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::wrap_pyfunction;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Default)]
struct QueueState {
    chunks: VecDeque<Vec<u8>>,
    error: Option<String>,
    eof: bool,
}

struct SharedState {
    queue: Mutex<QueueState>,
    condvar: Condvar,
    stop_requested: AtomicBool,
    bytes_read: AtomicU64,
    chunks_read: AtomicU64,
}

impl SharedState {
    fn new() -> Self {
        Self {
            queue: Mutex::new(QueueState::default()),
            condvar: Condvar::new(),
            stop_requested: AtomicBool::new(false),
            bytes_read: AtomicU64::new(0),
            chunks_read: AtomicU64::new(0),
        }
    }

    fn push_chunk(&self, chunk: Vec<u8>) {
        self.bytes_read
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        self.chunks_read.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut queue) = self.queue.lock() {
            queue.chunks.push_back(chunk);
        }
        self.condvar.notify_all();
    }

    fn mark_eof(&self) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.eof = true;
        }
        self.condvar.notify_all();
    }

    fn mark_error(&self, error: String) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.error = Some(error);
        }
        self.condvar.notify_all();
    }
}

fn run_pump(
    stdout_fd: i32,
    log_path: String,
    read_chunk_bytes: usize,
    log_flush_bytes: usize,
    log_flush_interval_ms: u64,
    shared: Arc<SharedState>,
) {
    let duplicated_fd = unsafe { dup(stdout_fd) };
    if duplicated_fd < 0 {
        shared.mark_error("dup(stdout_fd) failed".to_string());
        return;
    }

    let mut stdout = unsafe { File::from_raw_fd(duplicated_fd) };
    let mut log_file = match OpenOptions::new().create(true).append(true).open(&log_path) {
        Ok(file) => file,
        Err(err) => {
            shared.mark_error(format!("open(log_path) failed: {err}"));
            return;
        }
    };

    let chunk_size = read_chunk_bytes.max(1);
    let flush_threshold = log_flush_bytes.max(1);
    let poll_timeout_ms = log_flush_interval_ms.min(i32::MAX as u64) as i32;
    let mut buffer = vec![0_u8; chunk_size];
    let mut pending_flush_bytes = 0_usize;

    loop {
        if shared.stop_requested.load(Ordering::Relaxed) {
            break;
        }

        let mut poll_fd = pollfd {
            fd: duplicated_fd,
            events: POLLIN,
            revents: 0,
        };

        let poll_result = unsafe { poll(&mut poll_fd, 1, poll_timeout_ms) };
        if poll_result < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            shared.mark_error(format!("poll failed: {err}"));
            break;
        }

        if poll_result == 0 {
            if pending_flush_bytes > 0 {
                if let Err(err) = log_file.flush() {
                    shared.mark_error(format!("log flush failed: {err}"));
                    break;
                }
                pending_flush_bytes = 0;
            }
            continue;
        }

        if poll_fd.revents & POLLNVAL != 0 {
            shared.mark_error("poll returned POLLNVAL".to_string());
            break;
        }

        if poll_fd.revents & POLLERR != 0 {
            shared.mark_error("poll returned POLLERR".to_string());
            break;
        }

        if poll_fd.revents & (POLLIN | POLLHUP) == 0 {
            continue;
        }

        match stdout.read(&mut buffer) {
            Ok(0) => {
                if pending_flush_bytes > 0 {
                    if let Err(err) = log_file.flush() {
                        shared.mark_error(format!("log flush on eof failed: {err}"));
                        break;
                    }
                }
                shared.mark_eof();
                break;
            }
            Ok(read_count) => {
                if let Err(err) = log_file.write_all(&buffer[..read_count]) {
                    shared.mark_error(format!("log write failed: {err}"));
                    break;
                }
                pending_flush_bytes += read_count;
                if pending_flush_bytes >= flush_threshold {
                    if let Err(err) = log_file.flush() {
                        shared.mark_error(format!("log flush failed: {err}"));
                        break;
                    }
                    pending_flush_bytes = 0;
                }
                shared.push_chunk(buffer[..read_count].to_vec());
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                shared.mark_error(format!("stdout read failed: {err}"));
                break;
            }
        }
    }

    if pending_flush_bytes > 0 {
        let _ = log_file.flush();
    }
    shared.mark_eof();
}

#[pyclass]
struct NativePipePump {
    shared: Arc<SharedState>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl NativePipePump {
    fn drain_internal(&self, max_items: Option<usize>) -> Vec<Vec<u8>> {
        let limit = max_items.unwrap_or(usize::MAX).max(1);
        let mut out = Vec::new();
        if let Ok(mut queue) = self.shared.queue.lock() {
            for _ in 0..limit {
                match queue.chunks.pop_front() {
                    Some(chunk) => out.push(chunk),
                    None => break,
                }
            }
        }
        out
    }
}

impl Drop for NativePipePump {
    fn drop(&mut self) {
        self.shared.stop_requested.store(true, Ordering::Relaxed);
        self.shared.condvar.notify_all();
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(handle) = worker.take() {
                let _ = handle.join();
            }
        }
    }
}

#[pymethods]
impl NativePipePump {
    #[new]
    fn new(
        stdout_fd: i32,
        log_path: String,
        read_chunk_bytes: usize,
        log_flush_bytes: usize,
        log_flush_interval_ms: u64,
    ) -> PyResult<Self> {
        if stdout_fd < 0 {
            return Err(PyOSError::new_err("stdout_fd must be non-negative"));
        }

        let shared = Arc::new(SharedState::new());
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("fws-native-pipe-pump".to_string())
            .spawn(move || {
                run_pump(
                    stdout_fd,
                    log_path,
                    read_chunk_bytes,
                    log_flush_bytes,
                    log_flush_interval_ms,
                    worker_shared,
                )
            })
            .map_err(|err| PyRuntimeError::new_err(format!("failed to spawn native pipe pump: {err}")))?;

        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn stop(&self) {
        self.shared.stop_requested.store(true, Ordering::Relaxed);
        self.shared.condvar.notify_all();
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(handle) = worker.take() {
                let _ = handle.join();
            }
        }
    }

    #[pyo3(signature = (max_items=None))]
    fn drain_chunks(&self, py: Python<'_>, max_items: Option<usize>) -> Vec<Py<PyBytes>> {
        self.drain_internal(max_items)
            .into_iter()
            .map(|chunk| PyBytes::new_bound(py, &chunk).unbind())
            .collect()
    }

    #[pyo3(signature = (max_items=None, timeout_ms=0))]
    fn wait_for_chunks(
        &self,
        py: Python<'_>,
        max_items: Option<usize>,
        timeout_ms: u64,
    ) -> Vec<Py<PyBytes>> {
        let timeout = Duration::from_millis(timeout_ms);
        if let Ok(queue) = self.shared.queue.lock() {
            if queue.chunks.is_empty() && queue.error.is_none() && !queue.eof {
                let _ = self.shared.condvar.wait_timeout(queue, timeout);
            }
        }
        self.drain_internal(max_items)
            .into_iter()
            .map(|chunk| PyBytes::new_bound(py, &chunk).unbind())
            .collect()
    }

    fn is_finished(&self) -> bool {
        if let Ok(queue) = self.shared.queue.lock() {
            return queue.eof || queue.error.is_some() || self.shared.stop_requested.load(Ordering::Relaxed);
        }
        true
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new_bound(py);
        let (queue_len, eof, error) = if let Ok(queue) = self.shared.queue.lock() {
            (queue.chunks.len(), queue.eof, queue.error.clone())
        } else {
            (0, true, Some("queue lock poisoned".to_string()))
        };
        dict.set_item("bytes_read", self.shared.bytes_read.load(Ordering::Relaxed))?;
        dict.set_item("chunks_read", self.shared.chunks_read.load(Ordering::Relaxed))?;
        dict.set_item("queue_len", queue_len)?;
        dict.set_item("eof", eof)?;
        dict.set_item("error", error)?;
        dict.set_item(
            "stop_requested",
            self.shared.stop_requested.load(Ordering::Relaxed),
        )?;
        Ok(dict.unbind())
    }
}

#[pyfunction]
fn phase0_marker() -> &'static str {
    "native_pipe_testing_phase0"
}

#[pyfunction]
fn extension_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn fws_pipe_pump(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__phase__", "phase1")?;
    module.add_class::<NativePipePump>()?;
    module.add_function(wrap_pyfunction!(phase0_marker, module)?)?;
    module.add_function(wrap_pyfunction!(extension_version, module)?)?;
    Ok(())
}
