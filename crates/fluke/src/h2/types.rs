use std::{collections::HashMap, fmt};

use fluke_buffet::Piece;

use crate::Response;

use super::{
    body::H2BodySender,
    parse::{FrameType, KnownErrorCode, Settings, StreamId},
};

pub(crate) struct ConnState {
    pub(crate) streams: HashMap<StreamId, StreamState>,
    pub(crate) last_stream_id: StreamId,
    pub(crate) self_settings: Settings,
    pub(crate) peer_settings: Settings,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            streams: Default::default(),
            last_stream_id: StreamId(0),
            self_settings: Default::default(),
            peer_settings: Default::default(),
        }
    }
}

// cf. RFC 9113, 5.1 Stream States:
//
//                               +--------+
//                       send PP |        | recv PP
//                      ,--------+  idle  +--------.
//                     /         |        |         \
//                    v          +--------+          v
//             +----------+          |           +----------+
//             |          |          | send H /  |          |
//      ,------+ reserved |          | recv H    | reserved +------.
//      |      | (local)  |          |           | (remote) |      |
//      |      +---+------+          v           +------+---+      |
//      |          |             +--------+             |          |
//      |          |     recv ES |        | send ES     |          |
//      |   send H |     ,-------+  open  +-------.     | recv H   |
//      |          |    /        |        |        \    |          |
//      |          v   v         +---+----+         v   v          |
//      |      +----------+          |           +----------+      |
//      |      |   half-  |          |           |   half-  |      |
//      |      |  closed  |          | send R /  |  closed  |      |
//      |      | (remote) |          | recv R    | (local)  |      |
//      |      +----+-----+          |           +-----+----+      |
//      |           |                |                 |           |
//      |           | send ES /      |       recv ES / |           |
//      |           |  send R /      v        send R / |           |
//      |           |  recv R    +--------+   recv R   |           |
//      | send R /  `----------->|        |<-----------'  send R / |
//      | recv R                 | closed |               recv R   |
//      `----------------------->|        |<-----------------------'
//                               +--------+
//
//                         Figure 2: Stream States
//
//  send:  endpoint sends this frame
//  recv:  endpoint receives this frame
//  H:  HEADERS frame (with implied CONTINUATION frames)
//  ES:  END_STREAM flag
//  R:  RST_STREAM frame
//  PP:  PUSH_PROMISE frame (with implied CONTINUATION frames); state
//     transitions are for the promised stream
pub(crate) enum StreamState {
    // we have received full HEADERS
    Open(H2BodySender),

    // the peer has sent END_STREAM/RST_STREAM
    HalfClosedRemote,

    // we have sent END_STREAM/RST_STREAM
    HalfClosedLocal(H2BodySender),
    //
    // Note: the "Closed" state is indicated by not having an entry in the map
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum H2ConnectionError {
    #[error("frame too large: {frame_type:?} frame of size {frame_size} exceeds max frame size of {max_frame_size}")]
    FrameTooLarge {
        frame_type: FrameType,
        frame_size: u32,
        max_frame_size: u32,
    },

    #[error("remote hung up while reading payload of {frame_type:?} with length {frame_size}")]
    IncompleteFrame {
        frame_type: FrameType,
        frame_size: u32,
    },

    #[error("headers frame had invalid priority: stream {stream_id} depends on itself")]
    HeadersInvalidPriority { stream_id: StreamId },

    #[error("client tried to initiate an even-numbered stream")]
    ClientSidShouldBeOdd,

    #[error("client stream IDs should be numerically increasing")]
    ClientSidShouldBeNumericallyIncreasing {
        stream_id: StreamId,
        last_stream_id: StreamId,
    },

    #[error("received {frame_type:?} frame with Padded flag but empty payload")]
    PaddedFrameEmpty { frame_type: FrameType },

    #[error("received {frame_type:?} with Padded flag but payload was shorter than padding")]
    PaddedFrameTooShort {
        frame_type: FrameType,
        padding_length: usize,
        frame_size: u32,
    },

    #[error("on stream {stream_id}, expected continuation frame, but got {frame_type:?}")]
    ExpectedContinuationFrame {
        stream_id: StreamId,
        frame_type: Option<FrameType>,
    },

    #[error("expected continuation from for stream {stream_id}, but got continuation for stream {continuation_stream_id}")]
    ExpectedContinuationForStream {
        stream_id: StreamId,
        continuation_stream_id: StreamId,
    },

    #[error("on stream {stream_id}, received unexpected continuation frame")]
    UnexpectedContinuationFrame { stream_id: StreamId },

    #[error("compression error: {0:?}")]
    // FIXME: let's not use String, let's just replicate the enum from `fluke-hpack` or fix it?
    CompressionError(String),

    #[error("client sent a push promise frame, clients aren't allowed to do that, cf. RFC9113 section 8.4")]
    ClientSentPushPromise,

    #[error("received window update for unknown stream {stream_id}")]
    WindowUpdateForUnknownStream { stream_id: StreamId },

    #[error("other error: {0:?}")]
    Internal(#[from] eyre::Report),

    #[error("error reading/parsing H2 frame: {0:?}")]
    ReadError(eyre::Report),

    #[error("error writing H2 frame: {0:?}")]
    WriteError(std::io::Error),

    #[error("received rst frame for unknown stream")]
    RstStreamForUnknownStream { stream_id: StreamId },

    #[error("received frame for closed stream {stream_id}")]
    StreamClosed { stream_id: StreamId },

    #[error("received ping frame frame with non-zero stream id")]
    PingFrameWithNonZeroStreamId { stream_id: StreamId },

    #[error("received ping frame with invalid length {len}")]
    PingFrameInvalidLength { len: u32 },

    #[error("received settings frame with invalid length {len}")]
    SettingsAckWithPayload { len: u32 },

    #[error("received settings frame with non-zero stream id")]
    SettingsWithNonZeroStreamId { stream_id: StreamId },

    #[error("received goaway frame with non-zero stream id")]
    GoAwayWithNonZeroStreamId { stream_id: StreamId },

    #[error("zero increment in window update frame for stream")]
    WindowUpdateZeroIncrement,

    #[error("received window update frame with invalid length {len}")]
    WindowUpdateInvalidLength { len: usize },
}

impl H2ConnectionError {
    pub(crate) fn as_known_error_code(&self) -> KnownErrorCode {
        match self {
            // frame size errors
            H2ConnectionError::FrameTooLarge { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::PaddedFrameEmpty { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::PaddedFrameTooShort { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::PingFrameInvalidLength { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::SettingsAckWithPayload { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::WindowUpdateInvalidLength { .. } => KnownErrorCode::FrameSizeError,
            // compression errors
            H2ConnectionError::CompressionError(_) => KnownErrorCode::CompressionError,
            // stream closed error
            H2ConnectionError::StreamClosed { .. } => KnownErrorCode::StreamClosed,
            // internal errors
            H2ConnectionError::Internal(_) => KnownErrorCode::InternalError,
            // protocol errors
            _ => KnownErrorCode::ProtocolError,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum H2StreamError {
    #[allow(dead_code)]
    #[error("received {data_length} bytes in data frames but content-length announced {content_length} bytes")]
    DataLengthDoesNotMatchContentLength {
        data_length: u64,
        content_length: u64,
    },

    #[error("refused stream (would exceed max concurrent streams)")]
    RefusedStream,

    #[error("trailers must have EndStream flag set")]
    TrailersNotEndStream,

    #[error("received RST_STREAM frame")]
    ReceivedRstStream,

    #[error("received PRIORITY frame with invalid size")]
    InvalidPriorityFrameSize { frame_size: u32 },

    #[error("stream closed")]
    StreamClosed,

    #[error("received RST_STREAM frame with invalid size, expected 4 got {frame_size}")]
    InvalidRstStreamFrameSize { frame_size: u32 },
}

impl H2StreamError {
    pub(crate) fn as_known_error_code(&self) -> KnownErrorCode {
        use H2StreamError::*;
        use KnownErrorCode as Code;

        match self {
            StreamClosed => Code::StreamClosed,
            RefusedStream => Code::RefusedStream,
            InvalidPriorityFrameSize { .. } => Code::FrameSizeError,
            InvalidRstStreamFrameSize { .. } => Code::FrameSizeError,
            _ => Code::ProtocolError,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HeadersOrTrailers {
    Headers,
    Trailers,
}

#[derive(Debug)]
pub(crate) struct H2Event {
    pub(crate) stream_id: StreamId,
    pub(crate) payload: H2EventPayload,
}

pub(crate) enum H2EventPayload {
    Headers(Response),
    BodyChunk(Piece),
    BodyEnd,
}

impl fmt::Debug for H2EventPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Headers(_) => f.debug_tuple("Headers").finish(),
            Self::BodyChunk(_) => f.debug_tuple("BodyChunk").finish(),
            Self::BodyEnd => write!(f, "BodyEnd"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("the peer closed the connection unexpectedly")]
pub(crate) struct ConnectionClosed;
