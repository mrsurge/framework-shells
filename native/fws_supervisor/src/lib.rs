//! Experimental shared supervisor; not yet integrated into FWS manager launch paths.
pub mod client;
pub mod policy;
pub mod service;
pub mod session;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum frame payload; the four-byte network-order length is not included.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Protocol-neutral framing. A later authenticated session defines message types.
/// Never interpret disconnect here as child EOF or replay a failed write.
pub fn framed<T: AsyncRead + AsyncWrite>(transport: T) -> Framed<T, LengthDelimitedCodec> {
    LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(MAX_FRAME_BYTES)
        .new_framed(transport)
}
