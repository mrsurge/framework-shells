//! Optional experimental binding. Hot-path completions cross a readiness FD;
//! Tokio worker threads never enter Python to deliver output or write ACKs.
use fws_supervisor::client::{Client, ClientError, Event};
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyBytes};
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

fn error(error: ClientError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
struct Completion {
    id: u64,
    result: Result<usize, ClientError>,
    _permit: OwnedSemaphorePermit,
}
struct State {
    input: Option<UnixStream>,
    output: Option<UnixStream>,
    notified: bool,
    events: VecDeque<Event>,
    replies: VecDeque<Completion>,
    failure: Option<String>,
}
impl State {
    fn notify(&mut self) {
        if !self.notified {
            let Some(output) = &mut self.output else {
                return;
            };
            match output.write(&[1]) {
                Ok(_) => self.notified = true,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => self.notified = true,
                Err(e) => self.failure = Some(format!("closed: readiness FD failed: {e}")),
            }
        }
    }
}
struct Bridge {
    state: Mutex<State>,
    permits: Arc<Semaphore>,
    next: AtomicU64,
}
impl Bridge {
    fn new() -> std::io::Result<Self> {
        let (input, output) = UnixStream::pair()?;
        input.set_nonblocking(true)?;
        output.set_nonblocking(true)?;
        Ok(Self {
            state: Mutex::new(State {
                input: Some(input),
                output: Some(output),
                notified: false,
                events: VecDeque::new(),
                replies: VecDeque::new(),
                failure: None,
            }),
            permits: Arc::new(Semaphore::new(64)),
            next: AtomicU64::new(1),
        })
    }
    fn fail(&self, message: String) {
        let mut state = self.state.lock().unwrap();
        if state.failure.is_none() {
            state.failure = Some(message);
        }
        state.notify();
    }
    fn event(&self, event: Event) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.events.len() >= 256 {
            state.failure =
                Some("lagged: native binding output queue full; reconnect explicitly".into());
            state.notify();
            return false;
        }
        state.events.push_back(event);
        state.notify();
        true
    }
    fn complete(&self, completion: Completion) {
        let mut state = self.state.lock().unwrap();
        state.replies.push_back(completion); // Permit held until drain bounds completed replies too.
        state.notify();
    }
}

#[pyclass(frozen)]
struct NativeClient {
    client: Client,
    bridge: Arc<Bridge>,
    reader: tokio::task::AbortHandle,
}
impl Drop for NativeClient {
    fn drop(&mut self) {
        self.reader.abort();
        let client = self.client.clone();
        get_runtime().spawn(async move {
            client.close().await;
        });
    }
}
type EventTuple = (String, String, u64, Py<PyBytes>, Option<i64>);
type ReplyTuple = (u64, Option<usize>, Option<String>);
type Drain = (Vec<EventTuple>, Vec<ReplyTuple>, Option<String>);
#[pymethods]
impl NativeClient {
    #[staticmethod]
    fn connect(
        py: Python<'_>,
        path: String,
        secret: Vec<u8>,
        instance: String,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let bridge =
                Arc::new(Bridge::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))?);
            let client = Client::connect(path, &secret, &instance)
                .await
                .map_err(error)?;
            let output = bridge.clone();
            let reader_client = client.clone();
            let reader = tokio::spawn(async move {
                loop {
                    match reader_client.read().await {
                        Ok(event) => {
                            if !output.event(event) {
                                reader_client.close().await;
                                break;
                            }
                        }
                        Err(error) => {
                            output.fail(error.to_string());
                            break;
                        }
                    }
                }
            })
            .abort_handle();
            Ok(Self {
                client,
                bridge,
                reader,
            })
        })
    }
    fn reader_fd(&self) -> i32 {
        self.bridge
            .state
            .lock()
            .unwrap()
            .input
            .as_ref()
            .map_or(-1, AsRawFd::as_raw_fd)
    }
    fn close_fd(&self) {
        let mut state = self.bridge.state.lock().unwrap();
        state.input.take();
        state.output.take();
    }
    fn write_submit(&self, data: &[u8]) -> PyResult<u64> {
        if data.len() > 32768 {
            return Err(PyRuntimeError::new_err(
                "invalid: write exceeds 32768 bytes; not sent",
            ));
        }
        let permit = self
            .bridge
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                PyRuntimeError::new_err("busy: binding write queue full; command not sent")
            })?;
        let id = self.bridge.next.fetch_add(1, Ordering::Relaxed);
        let data = data.to_vec();
        let client = self.client.clone();
        let bridge = self.bridge.clone();
        get_runtime().spawn(async move {
            let result = client.write(&data).await;
            bridge.complete(Completion {
                id,
                result,
                _permit: permit,
            });
        });
        Ok(id)
    }
    fn drain(&self, py: Python<'_>) -> Drain {
        let (events, replies, failure) = {
            let mut state = self.bridge.state.lock().unwrap();
            let mut bytes = [0u8; 64];
            while let Some(input) = &mut state.input {
                match input.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(_) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
            state.notified = false;
            (
                std::mem::take(&mut state.events),
                std::mem::take(&mut state.replies),
                state.failure.clone(),
            )
        };
        let events = events
            .into_iter()
            .map(|event| match event {
                Event::Output {
                    stream,
                    offset,
                    data,
                } => (
                    "output".into(),
                    stream,
                    offset,
                    PyBytes::new(py, &data).unbind(),
                    None,
                ),
                Event::Exited { code } => (
                    "exited".into(),
                    String::new(),
                    0,
                    PyBytes::new(py, b"").unbind(),
                    code,
                ),
            })
            .collect();
        let replies = replies
            .into_iter()
            .map(|reply| match reply.result {
                Ok(count) => (reply.id, Some(count), None),
                Err(error) => (reply.id, None, Some(error.to_string())),
            })
            .collect();
        (events, replies, failure)
    }
    fn acquire(&self, py: Python<'_>, takeover: bool) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        Ok(future_into_py(
            py,
            async move { client.acquire(takeover).await.map_err(error) },
        )?
        .unbind())
    }
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        Ok(future_into_py(py, async move {
            let status = client.status().await.map_err(error)?;
            Ok((
                status.pid,
                status.instance,
                status.exited,
                status.code,
                status.failure,
            ))
        })?
        .unbind())
    }
    fn eof(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        Ok(future_into_py(py, async move { client.eof().await.map_err(error) })?.unbind())
    }
    fn detach(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        Ok(future_into_py(py, async move { client.detach().await.map_err(error) })?.unbind())
    }
    fn stop(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        Ok(future_into_py(py, async move { client.stop().await.map_err(error) })?.unbind())
    }
    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let client = self.client.clone();
        let bridge = self.bridge.clone();
        let reader = self.reader.clone();
        Ok(future_into_py(py, async move {
            client.close().await;
            reader.abort();
            bridge.fail("closed: client closed".into());
            Ok(())
        })?
        .unbind())
    }
}
#[pymodule(gil_used = false)]
fn fws_supervisor_client(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();
    pyo3_async_runtimes::tokio::init(builder);
    module.add("API_VERSION", 2)?;
    module.add_class::<NativeClient>()
}
