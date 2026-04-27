use libc::{close, dup, fcntl, FD_CLOEXEC, F_GETFD, F_GETFL, F_SETFD, F_SETFL, O_NONBLOCK};
use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::wrap_pyfunction;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct EndpointState {
    stdout: Option<File>,
    log_file: Option<File>,
    pending_flush_bytes: usize,
    last_flush_at: Instant,
    eof: bool,
    error: Option<String>,
}

impl EndpointState {
    fn new(stdout: File, log_file: File) -> Self {
        Self {
            stdout: Some(stdout),
            log_file: Some(log_file),
            pending_flush_bytes: 0,
            last_flush_at: Instant::now(),
            eof: false,
            error: None,
        }
    }

    fn flush_log(&mut self) -> Result<(), String> {
        if self.pending_flush_bytes == 0 {
            return Ok(());
        }
        let Some(log_file) = self.log_file.as_mut() else {
            self.pending_flush_bytes = 0;
            self.last_flush_at = Instant::now();
            return Ok(());
        };
        log_file
            .flush()
            .map_err(|err| format!("log flush failed: {err}"))?;
        self.pending_flush_bytes = 0;
        self.last_flush_at = Instant::now();
        Ok(())
    }

    fn stop(&mut self) {
        let _ = self.flush_log();
        self.stdout = None;
        self.log_file = None;
        self.eof = true;
    }
}

fn set_nonblocking_cloexec(fd: i32) -> Result<(), String> {
    let current_flags = unsafe { fcntl(fd, F_GETFL) };
    if current_flags < 0 {
        return Err(format!("fcntl(F_GETFL) failed: {}", std::io::Error::last_os_error()));
    }
    if unsafe { fcntl(fd, F_SETFL, current_flags | O_NONBLOCK) } < 0 {
        return Err(format!("fcntl(F_SETFL) failed: {}", std::io::Error::last_os_error()));
    }

    let current_fd_flags = unsafe { fcntl(fd, F_GETFD) };
    if current_fd_flags < 0 {
        return Err(format!("fcntl(F_GETFD) failed: {}", std::io::Error::last_os_error()));
    }
    if unsafe { fcntl(fd, F_SETFD, current_fd_flags | FD_CLOEXEC) } < 0 {
        return Err(format!("fcntl(F_SETFD) failed: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

#[pyclass]
struct NativePipePump {
    state: Mutex<EndpointState>,
    bytes_read: AtomicU64,
    chunks_read: AtomicU64,
    read_chunk_bytes: usize,
    log_flush_bytes: usize,
    log_flush_interval: Duration,
}

impl Drop for NativePipePump {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.stop();
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

        let duplicated_fd = unsafe { dup(stdout_fd) };
        if duplicated_fd < 0 {
            return Err(PyOSError::new_err(format!(
                "dup(stdout_fd) failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        if let Err(err) = set_nonblocking_cloexec(duplicated_fd) {
            unsafe {
                close(duplicated_fd);
            }
            return Err(PyRuntimeError::new_err(err));
        }

        let stdout = unsafe { File::from_raw_fd(duplicated_fd) };
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|err| PyRuntimeError::new_err(format!("open(log_path) failed: {err}")))?;

        Ok(Self {
            state: Mutex::new(EndpointState::new(stdout, log_file)),
            bytes_read: AtomicU64::new(0),
            chunks_read: AtomicU64::new(0),
            read_chunk_bytes: read_chunk_bytes.max(1),
            log_flush_bytes: log_flush_bytes.max(1),
            log_flush_interval: Duration::from_millis(log_flush_interval_ms.max(1)),
        })
    }

    fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stop();
        }
    }

    fn reader_fd(&self) -> i32 {
        let Ok(state) = self.state.lock() else {
            return -1;
        };
        state
            .stdout
            .as_ref()
            .map(AsRawFd::as_raw_fd)
            .unwrap_or(-1)
    }

    #[pyo3(signature = (max_items=None))]
    fn read_available(&self, py: Python<'_>, max_items: Option<usize>) -> Vec<Py<PyBytes>> {
        let limit = max_items.unwrap_or(usize::MAX).max(1);
        let mut out = Vec::new();
        let mut buffer = vec![0_u8; self.read_chunk_bytes];

        let Ok(mut state) = self.state.lock() else {
            return out;
        };
        if state.eof || state.error.is_some() {
            return out;
        }

        for _ in 0..limit {
            let read_result = {
                let Some(stdout) = state.stdout.as_mut() else {
                    state.eof = true;
                    break;
                };
                stdout.read(&mut buffer)
            };

            match read_result {
                Ok(0) => {
                    let flush_result = state.flush_log();
                    if let Err(err) = flush_result {
                        state.error = Some(err);
                    }
                    state.eof = true;
                    break;
                }
                Ok(read_count) => {
                    if let Some(log_file) = state.log_file.as_mut() {
                        if let Err(err) = log_file.write_all(&buffer[..read_count]) {
                            state.error = Some(format!("log write failed: {err}"));
                            break;
                        }
                    }
                    state.pending_flush_bytes += read_count;
                    if state.pending_flush_bytes >= self.log_flush_bytes {
                        if let Err(err) = state.flush_log() {
                            state.error = Some(err);
                            break;
                        }
                    }
                    self.bytes_read
                        .fetch_add(read_count as u64, Ordering::Relaxed);
                    self.chunks_read.fetch_add(1, Ordering::Relaxed);
                    out.push(PyBytes::new_bound(py, &buffer[..read_count]).unbind());
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    if state.pending_flush_bytes > 0
                        && state.last_flush_at.elapsed() >= self.log_flush_interval
                    {
                        if let Err(flush_err) = state.flush_log() {
                            state.error = Some(flush_err);
                        }
                    }
                    break;
                }
                Err(err) => {
                    state.error = Some(format!("stdout read failed: {err}"));
                    let _ = state.flush_log();
                    break;
                }
            }
        }

        out
    }

    fn is_finished(&self) -> bool {
        let Ok(state) = self.state.lock() else {
            return true;
        };
        state.eof || state.error.is_some()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new_bound(py);
        let (reader_fd, eof, error, pending_flush_bytes) = if let Ok(state) = self.state.lock() {
            (
                state
                    .stdout
                    .as_ref()
                    .map(AsRawFd::as_raw_fd)
                    .unwrap_or(-1),
                state.eof,
                state.error.clone(),
                state.pending_flush_bytes,
            )
        } else {
            (-1, true, Some("state lock poisoned".to_string()), 0)
        };
        dict.set_item("bytes_read", self.bytes_read.load(Ordering::Relaxed))?;
        dict.set_item("chunks_read", self.chunks_read.load(Ordering::Relaxed))?;
        dict.set_item("reader_fd", reader_fd)?;
        dict.set_item("eof", eof)?;
        dict.set_item("error", error)?;
        dict.set_item("pending_flush_bytes", pending_flush_bytes)?;
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
