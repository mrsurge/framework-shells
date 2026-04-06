use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

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
    module.add("__phase__", "phase0")?;
    module.add_function(wrap_pyfunction!(phase0_marker, module)?)?;
    module.add_function(wrap_pyfunction!(extension_version, module)?)?;
    Ok(())
}
