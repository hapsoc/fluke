//! HTTP/2 parser
//!
//! HTTP/2 <https://httpwg.org/specs/rfc9113.html>
//! HTTP semantics <https://httpwg.org/specs/rfc9110.html>

use std::{fmt, ops::RangeInclusive};

use enum_repr::EnumRepr;
use enumflags2::{bitflags, BitFlags};
use nom::{
    combinator::map,
    number::streaming::{be_u16, be_u24, be_u32, be_u8},
    sequence::tuple,
    IResult,
};

use fluke_buffet::{Roll, RollMut};

/// This is sent by h2 clients after negotiating over ALPN, or when doing h2c.
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub fn preface(i: Roll) -> IResult<Roll, ()> {
    let (i, _) = nom::bytes::streaming::tag(PREFACE)(i)?;
    Ok((i, ()))
}

/// See https://httpwg.org/specs/rfc9113.html#FrameTypes
#[EnumRepr(type = "u8")]
#[derive(Debug, Clone, Copy)]
pub enum RawFrameType {
    Data = 0x00,
    Headers = 0x01,
    Priority = 0x02,
    RstStream = 0x03,
    Settings = 0x04,
    PushPromise = 0x05,
    Ping = 0x06,
    GoAway = 0x07,
    WindowUpdate = 0x08,
    Continuation = 0x09,
}

/// Typed flags for various frame types
#[derive(Debug, Clone, Copy)]
pub enum FrameType {
    Data(BitFlags<DataFlags>),
    Headers(BitFlags<HeadersFlags>),
    Priority,
    RstStream,
    Settings(BitFlags<SettingsFlags>),
    PushPromise,
    Ping(BitFlags<PingFlags>),
    GoAway,
    WindowUpdate,
    Continuation(BitFlags<ContinuationFlags>),
    Unknown(EncodedFrameType),
}

/// See https://httpwg.org/specs/rfc9113.html#DATA
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DataFlags {
    Padded = 0x08,
    EndStream = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#rfc.section.6.2
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HeadersFlags {
    Priority = 0x20,
    Padded = 0x08,
    EndHeaders = 0x04,
    EndStream = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#SETTINGS
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SettingsFlags {
    Ack = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#PING
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PingFlags {
    Ack = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#CONTINUATION
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ContinuationFlags {
    EndHeaders = 0x04,
}

#[derive(Debug, Clone, Copy)]
pub struct EncodedFrameType {
    pub ty: u8,
    pub flags: u8,
}

impl EncodedFrameType {
    fn parse(i: Roll) -> IResult<Roll, Self> {
        let (i, (ty, flags)) = tuple((be_u8, be_u8))(i)?;
        Ok((i, Self { ty, flags }))
    }
}

impl From<(RawFrameType, u8)> for EncodedFrameType {
    fn from((ty, flags): (RawFrameType, u8)) -> Self {
        Self {
            ty: ty.repr(),
            flags,
        }
    }
}

impl FrameType {
    pub(crate) fn encode(self) -> EncodedFrameType {
        match self {
            FrameType::Data(f) => (RawFrameType::Data, f.bits()).into(),
            FrameType::Headers(f) => (RawFrameType::Headers, f.bits()).into(),
            FrameType::Priority => (RawFrameType::Priority, 0).into(),
            FrameType::RstStream => (RawFrameType::RstStream, 0).into(),
            FrameType::Settings(f) => (RawFrameType::Settings, f.bits()).into(),
            FrameType::PushPromise => (RawFrameType::PushPromise, 0).into(),
            FrameType::Ping(f) => (RawFrameType::Ping, f.bits()).into(),
            FrameType::GoAway => (RawFrameType::GoAway, 0).into(),
            FrameType::WindowUpdate => (RawFrameType::WindowUpdate, 0).into(),
            FrameType::Continuation(f) => (RawFrameType::Continuation, f.bits()).into(),
            FrameType::Unknown(ft) => ft,
        }
    }

    fn decode(ft: EncodedFrameType) -> Self {
        match RawFrameType::from_repr(ft.ty) {
            Some(ty) => match ty {
                RawFrameType::Data => {
                    FrameType::Data(BitFlags::<DataFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::Headers => {
                    FrameType::Headers(BitFlags::<HeadersFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::Priority => FrameType::Priority,
                RawFrameType::RstStream => FrameType::RstStream,
                RawFrameType::Settings => {
                    FrameType::Settings(BitFlags::<SettingsFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::PushPromise => FrameType::PushPromise,
                RawFrameType::Ping => {
                    FrameType::Ping(BitFlags::<PingFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::GoAway => FrameType::GoAway,
                RawFrameType::WindowUpdate => FrameType::WindowUpdate,
                RawFrameType::Continuation => FrameType::Continuation(
                    BitFlags::<ContinuationFlags>::from_bits_truncate(ft.flags),
                ),
            },
            None => FrameType::Unknown(ft),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub(crate) u32);

impl StreamId {
    /// Stream ID used for connection control frames
    pub const CONNECTION: Self = Self(0);

    /// Server-initiated streams have even IDs
    pub fn is_server_initiated(&self) -> bool {
        self.0 % 2 == 0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid stream id: {0}")]
pub struct StreamIdOutOfRange(u32);

impl TryFrom<u32> for StreamId {
    type Error = StreamIdOutOfRange;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value & 0x8000_0000 != 0 {
            Err(StreamIdOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// See https://httpwg.org/specs/rfc9113.html#FrameHeader
pub struct Frame {
    pub frame_type: FrameType,
    pub reserved: u8,
    pub stream_id: StreamId,
    pub len: u32,
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.stream_id.0 == 0 {
            write!(f, "Conn:")?;
        } else {
            write!(f, "#{}:", self.stream_id.0)?;
        }

        let name = match &self.frame_type {
            FrameType::Data(_) => "Data",
            FrameType::Headers(_) => "Headers",
            FrameType::Priority => "Priority",
            FrameType::RstStream => "RstStream",
            FrameType::Settings(_) => "Settings",
            FrameType::PushPromise => "PushPromise",
            FrameType::Ping(_) => "Ping",
            FrameType::GoAway => "GoAway",
            FrameType::WindowUpdate => "WindowUpdate",
            FrameType::Continuation(_) => "Continuation",
            FrameType::Unknown(EncodedFrameType { ty, flags }) => {
                return write!(f, "UnknownFrame({:#x}, {:#x})", ty, flags)
            }
        };
        let mut s = f.debug_struct(name);

        if self.reserved != 0 {
            s.field("reserved", &self.reserved);
        }
        if self.len > 0 {
            s.field("len", &self.len);
        }

        // now write flags with DisplayDebug
        struct DisplayDebug<'a, D: fmt::Display>(&'a D);
        impl<'a, D: fmt::Display> fmt::Debug for DisplayDebug<'a, D> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self.0, f)
            }
        }

        // for all the variants with flags, add a flags field, of value
        // &DisplayDebug(flags)
        match &self.frame_type {
            FrameType::Data(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Headers(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Settings(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Ping(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Continuation(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            _ => {
                // muffin
            }
        }

        s.finish()
    }
}

impl Frame {
    /// Create a new frame with the given type and stream ID.
    pub fn new(frame_type: FrameType, stream_id: StreamId) -> Self {
        Self {
            frame_type,
            reserved: 0,
            stream_id,
            len: 0,
        }
    }

    /// Set the frame's length.
    pub fn with_len(mut self, len: u32) -> Self {
        self.len = len;
        self
    }

    /// Parse a frame from the given slice
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        let (i, (len, frame_type, (reserved, stream_id))) = tuple((
            be_u24,
            EncodedFrameType::parse,
            parse_reserved_and_stream_id,
        ))(i)?;

        let frame = Frame {
            frame_type: FrameType::decode(frame_type),
            reserved,
            stream_id,
            len,
        };
        Ok((i, frame))
    }

    pub fn write_into(self, mut w: impl std::io::Write) -> eyre::Result<()> {
        use byteorder::{BigEndian, WriteBytesExt};
        w.write_u24::<BigEndian>(self.len as _)?;
        let ft = self.frame_type.encode();
        w.write_u8(ft.ty)?;
        w.write_u8(ft.flags)?;
        // TODO: do we ever need to write the reserved bit?
        w.write_u32::<BigEndian>(self.stream_id.0)?;

        Ok(())
    }

    pub fn into_roll(self, mut scratch: &mut RollMut) -> eyre::Result<Roll> {
        debug_assert_eq!(scratch.len(), 0);
        self.write_into(&mut scratch)?;
        Ok(scratch.take_all())
    }
}

/// See https://httpwg.org/specs/rfc9113.html#FrameHeader - the first bit
/// is reserved, and the rest is a 31-bit stream id
pub fn parse_reserved_and_u31(i: Roll) -> IResult<Roll, (u8, u32)> {
    fn reserved(i: (Roll, usize)) -> IResult<(Roll, usize), u8> {
        nom::bits::streaming::take(1_usize)(i)
    }

    fn stream_id(i: (Roll, usize)) -> IResult<(Roll, usize), u32> {
        nom::bits::streaming::take(31_usize)(i)
    }

    nom::bits::bits(tuple((reserved, stream_id)))(i)
}

fn parse_reserved_and_stream_id(i: Roll) -> IResult<Roll, (u8, StreamId)> {
    parse_reserved_and_u31(i).map(|(i, (reserved, stream_id))| (i, (reserved, StreamId(stream_id))))
}

// cf. https://httpwg.org/specs/rfc9113.html#HEADERS
#[derive(Debug)]
pub(crate) struct PrioritySpec {
    pub exclusive: bool,
    pub stream_dependency: StreamId,
    // 0-255 => 1-256
    pub weight: u8,
}

impl PrioritySpec {
    pub(crate) fn parse(i: Roll) -> IResult<Roll, Self> {
        map(
            tuple((parse_reserved_and_stream_id, be_u8)),
            |((exclusive, stream_dependency), weight)| Self {
                exclusive: exclusive != 0,
                stream_dependency,
                weight,
            },
        )(i)
    }
}

#[derive(Clone, Copy)]
pub struct ErrorCode(u32);

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match KnownErrorCode::from_repr(self.0) {
            Some(e) => fmt::Debug::fmt(&e, f),
            None => write!(f, "ErrorCode(0x{:02x})", self.0),
        }
    }
}

impl From<KnownErrorCode> for ErrorCode {
    fn from(e: KnownErrorCode) -> Self {
        Self(e as u32)
    }
}

#[EnumRepr(type = "u32")]
#[derive(Debug, Clone, Copy)]
pub enum KnownErrorCode {
    /// The associated condition is not a result of an error. For example, a
    /// GOAWAY might include this code to indicate graceful shutdown of a
    /// connection.
    NoError = 0x00,

    /// The endpoint detected an unspecific protocol error. This error is for
    /// use when a more specific error code is not available.
    ProtocolError = 0x01,

    /// The endpoint encountered an unexpected internal error.
    InternalError = 0x02,

    /// The endpoint detected that its peer violated the flow-control protocol.
    FlowControlError = 0x03,

    /// The endpoint sent a SETTINGS frame but did not receive a response in a
    /// timely manner. See Section 6.5.3 ("Settings Synchronization").
    /// https://httpwg.org/specs/rfc9113.html#SettingsSync
    SettingsTimeout = 0x04,

    /// The endpoint received a frame after a stream was half-closed.
    StreamClosed = 0x05,

    /// The endpoint received a frame with an invalid size.
    FrameSizeError = 0x06,

    /// The endpoint refused the stream prior to performing any application
    /// processing (see Section 8.7 for details).
    /// https://httpwg.org/specs/rfc9113.html#Reliability
    RefusedStream = 0x07,

    /// The endpoint uses this error code to indicate that the stream is no
    /// longer needed.
    Cancel = 0x08,

    /// The endpoint is unable to maintain the field section compression context
    /// for the connection.
    CompressionError = 0x09,

    /// The connection established in response to a CONNECT request (Section
    /// 8.5) was reset or abnormally closed.
    /// https://httpwg.org/specs/rfc9113.html#CONNECT
    ConnectError = 0x0a,

    /// The endpoint detected that its peer is exhibiting a behavior that might
    /// be generating excessive load.
    EnhanceYourCalm = 0x0b,

    /// The underlying transport has properties that do not meet minimum
    /// security requirements (see Section 9.2).
    /// https://httpwg.org/specs/rfc9113.html#TLSUsage
    InadequateSecurity = 0x0c,

    /// The endpoint requires that HTTP/1.1 be used instead of HTTP/2.
    Http1_1Required = 0x0d,
}

impl TryFrom<ErrorCode> for KnownErrorCode {
    type Error = ();

    fn try_from(e: ErrorCode) -> Result<Self, Self::Error> {
        KnownErrorCode::from_repr(e.0).ok_or(())
    }
}

/// cf. https://httpwg.org/specs/rfc9113.html#SettingValues
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// This setting allows the sender to inform the remote endpoint of the
    /// maximum size of the compression table used to decode field blocks, in
    /// units of octets. The encoder can select any size equal to or less than
    /// this value by using signaling specific to the compression format inside
    /// a field block (see [COMPRESSION]). The initial value is 4,096 octets.
    pub header_table_size: u32,

    /// This setting can be used to enable or disable server push. A server MUST
    /// NOT send a PUSH_PROMISE frame if it receives this parameter set to a
    /// value of 0; see Section 8.4. A client that has both set this parameter
    /// to 0 and had it acknowledged MUST treat the receipt of a PUSH_PROMISE
    /// frame as a connection error (Section 5.4.1) of type PROTOCOL_ERROR.
    ///
    /// The initial value of SETTINGS_ENABLE_PUSH is 1. For a client, this value
    /// indicates that it is willing to receive PUSH_PROMISE frames. For a
    /// server, this initial value has no effect, and is equivalent to the value
    /// 0. Any value other than 0 or 1 MUST be treated as a connection error
    /// (Section 5.4.1) of type PROTOCOL_ERROR.
    ///
    /// A server MUST NOT explicitly set this value to 1. A server MAY choose to
    /// omit this setting when it sends a SETTINGS frame, but if a server does
    /// include a value, it MUST be 0. A client MUST treat receipt of a SETTINGS
    /// frame with SETTINGS_ENABLE_PUSH set to 1 as a connection error (Section
    /// 5.4.1) of type PROTOCOL_ERROR.
    pub enable_push: bool,

    /// This setting indicates the maximum number of concurrent streams that the
    /// sender will allow. This limit is directional: it applies to the number of
    /// streams that the sender permits the receiver to create. Initially, there is
    /// no limit to this value. It is recommended that this value be no smaller than
    /// 100, so as to not unnecessarily limit parallelism.
    ///
    /// A value of 0 for SETTINGS_MAX_CONCURRENT_STREAMS SHOULD NOT be treated as
    /// special by endpoints. A zero value does prevent the creation of new streams;
    /// however, this can also happen for any limit that is exhausted with active
    /// streams. Servers SHOULD only set a zero value for short durations; if a
    /// server does not wish to accept requests, closing the connection is more
    /// appropriate.
    pub max_concurrent_streams: u32,

    /// This setting indicates the sender's initial window size (in units of
    /// octets) for stream-level flow control. The initial value is 2^16-1
    /// (65,535) octets.
    ///
    /// This setting affects the window size of all streams (see Section 6.9.2).
    ///
    /// Values above the maximum flow-control window size of 2^31-1 MUST be
    /// treated as a connection error (Section 5.4.1) of type
    /// FLOW_CONTROL_ERROR.
    pub initial_window_size: u32,

    /// This setting indicates the size of the largest frame payload that the
    /// sender is willing to receive, in units of octets.
    ///
    /// The initial value is 2^14 (16,384) octets. The value advertised by an
    /// endpoint MUST be between this initial value and the maximum allowed frame
    /// size (2^24-1 or 16,777,215 octets), inclusive. Values outside this range MUST
    /// be treated as a connection error (Section 5.4.1) of type PROTOCOL_ERROR.
    pub max_frame_size: u32,

    /// This advisory setting informs a peer of the maximum field section size
    /// that the sender is prepared to accept, in units of octets. The value is
    /// based on the uncompressed size of field lines, including the length of
    /// the name and value in units of octets plus an overhead of 32 octets for
    /// each field line.
    ///
    /// For any given request, a lower limit than what is advertised MAY be
    /// enforced. The initial value of this setting is unlimited.
    pub max_header_list_size: u32,
}

impl Default for Settings {
    fn default() -> Self {
        // cf. https://httpwg.org/specs/rfc9113.html#SettingValues
        Self {
            header_table_size: 4096,
            enable_push: false,
            max_concurrent_streams: 100,
            initial_window_size: (1 << 16) - 1,
            max_frame_size: (1 << 14),
            max_header_list_size: 0,
        }
    }
}

#[EnumRepr(type = "u16")]
#[derive(Debug, Clone, Copy)]
enum SettingIdentifier {
    HeaderTableSize = 0x01,
    EnablePush = 0x02,
    MaxConcurrentStreams = 0x03,
    InitialWindowSize = 0x04,
    MaxFrameSize = 0x05,
    MaxHeaderListSize = 0x06,
}

impl Settings {
    const MAX_INITIAL_WINDOW_SIZE: u32 = (1 << 31) - 1;
    const MAX_FRAME_SIZE_ALLOWED_RANGE: RangeInclusive<u32> = (1 << 14)..=((1 << 24) - 1);

    pub fn parse(mut i: Roll) -> IResult<Roll, Self> {
        tracing::trace!("parsing settings frame, roll length: {}", i.len());
        let mut settings = Self::default();

        while !i.is_empty() {
            let (rest, (id, value)) = tuple((be_u16, be_u32))(i)?;
            tracing::trace!(%id, %value, "Got setting pair");
            match SettingIdentifier::from_repr(id) {
                None => {
                    // ignore unknown settings
                }
                Some(id) => match id {
                    SettingIdentifier::HeaderTableSize => {
                        settings.header_table_size = value;
                    }
                    SettingIdentifier::EnablePush => {
                        settings.enable_push = match value {
                            0 => false,
                            1 => true,
                            _ => {
                                return Err(nom::Err::Error(nom::error::Error::new(
                                    rest,
                                    nom::error::ErrorKind::Digit,
                                )));
                            }
                        }
                    }
                    SettingIdentifier::MaxConcurrentStreams => {
                        settings.max_concurrent_streams = value;
                    }
                    SettingIdentifier::InitialWindowSize => {
                        if value > Self::MAX_INITIAL_WINDOW_SIZE {
                            return Err(nom::Err::Error(nom::error::Error::new(
                                rest,
                                nom::error::ErrorKind::Digit,
                            )));
                        }
                        settings.initial_window_size = value;
                    }
                    SettingIdentifier::MaxFrameSize => {
                        if !Self::MAX_FRAME_SIZE_ALLOWED_RANGE.contains(&value) {
                            return Err(nom::Err::Error(nom::error::Error::new(
                                rest,
                                // FIXME: this isn't really representative of
                                // the quality error handling we're striving for
                                nom::error::ErrorKind::Digit,
                            )));
                        }
                        settings.max_frame_size = value;
                    }
                    SettingIdentifier::MaxHeaderListSize => {
                        settings.max_header_list_size = value;
                    }
                },
            }
            i = rest;
        }

        Ok((i, settings))
    }

    /// Iterates over pairs of id/values
    pub fn pairs(&self) -> impl Iterator<Item = (u16, u32)> {
        [
            (
                SettingIdentifier::HeaderTableSize as u16,
                self.header_table_size,
            ),
            (
                // note: as a server, we're free to omit this, but it doesn't
                // hurt to send 0 there I guess.
                SettingIdentifier::EnablePush as u16,
                self.enable_push as u32,
            ),
            (
                SettingIdentifier::MaxConcurrentStreams as u16,
                self.max_concurrent_streams,
            ),
            (
                SettingIdentifier::InitialWindowSize as u16,
                self.initial_window_size,
            ),
            (SettingIdentifier::MaxFrameSize as u16, self.max_frame_size),
            (
                SettingIdentifier::MaxHeaderListSize as u16,
                self.max_header_list_size,
            ),
        ]
        .into_iter()
    }

    /// Encode these settings into (u16, u32) pairs as specified in
    /// https://httpwg.org/specs/rfc9113.html#SETTINGS
    pub fn write_into(self, mut w: impl std::io::Write) -> eyre::Result<()> {
        use byteorder::{BigEndian, WriteBytesExt};

        for (id, value) in self.pairs() {
            w.write_u16::<BigEndian>(id)?;
            w.write_u32::<BigEndian>(value)?;
        }

        Ok(())
    }

    /// Same as `write_into` but uses a scratch [RollMut] to return a [Roll]
    pub fn into_roll(self, mut scratch: &mut RollMut) -> eyre::Result<Roll> {
        debug_assert_eq!(scratch.len(), 0);
        self.write_into(&mut scratch)?;
        Ok(scratch.take_all())
    }
}
