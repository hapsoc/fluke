use std::{collections::HashMap, rc::Rc};

use http::{header::HeaderName, Version};
use pretty_hex::PrettyHex;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    h2::parse::{DataFlags, Frame, FrameType, HeadersFlags, SettingsFlags, StreamId},
    util::read_and_parse,
    Headers, Method, Piece, PieceStr, ReadWriteOwned, Request, Roll, RollMut,
};

/// HTTP/2 server configuration
pub struct ServerConf {
    pub max_streams: u32,
}

impl Default for ServerConf {
    fn default() -> Self {
        Self { max_streams: 32 }
    }
}

pub(crate) mod parse;

#[derive(Default)]
struct ConnState {
    streams: HashMap<StreamId, StreamState>,
}

struct StreamState {
    rx_stage: StreamRxStage,
}

enum StreamRxStage {
    Headers(Headers),
    Body(mpsc::Sender<eyre::Result<Roll>>),
}

pub async fn serve(
    transport: impl ReadWriteOwned,
    conf: Rc<ServerConf>,
    mut client_buf: RollMut,
) -> eyre::Result<()> {
    debug!("TODO: enforce max_streams {}", conf.max_streams);

    let mut state = ConnState::default();

    const MAX_FRAME_LEN: usize = 64 * 1024;

    let transport = Rc::new(transport);
    let mut hpack_dec = hpack::Decoder::new();

    (client_buf, _) = match read_and_parse(
        parse::preface,
        transport.as_ref(),
        client_buf,
        parse::PREFACE.len(),
    )
    .await?
    {
        Some((client_buf, frame)) => (client_buf, frame),
        None => {
            debug!("h2 client closed connection before sending preface");
            return Ok(());
        }
    };
    debug!("read preface");

    loop {
        let frame;
        (client_buf, frame) =
            match read_and_parse(Frame::parse, transport.as_ref(), client_buf, 32 * 1024).await? {
                Some((client_buf, frame)) => (client_buf, frame),
                None => {
                    debug!("h2 client closed connection");
                    return Ok(());
                }
            };

        debug!(?frame, "received h2 frame");

        match frame.frame_type {
            FrameType::Data(flags) => {
                if flags.contains(DataFlags::Padded) {
                    todo!("handle padded data frame");
                }

                let mut remain = frame.len as usize;
                while remain > 0 {
                    debug!("reading frame payload, {remain} remains");

                    if client_buf.is_empty() {
                        let res;
                        (res, client_buf) = client_buf.read_into(remain, transport.as_ref()).await;
                        let n = res?;
                        debug!("read {n} bytes for frame payload");
                    }
                    let roll = client_buf.take_at_most(remain).unwrap();
                    debug!("read {} of frame payload", roll.len());
                    remain -= roll.len();
                }
                debug!("done reading frame payload");

                // TODO: handle `EndStream`
            }
            FrameType::Headers(flags) => {
                if flags.contains(HeadersFlags::Padded) {
                    todo!("padded headers are not supported");
                }

                if state.streams.contains_key(&frame.stream_id) {
                    todo!("handle connection error: received headers for existing stream");
                }

                let mut headers = Headers::default();
                debug!("receiving headers for stream {}", frame.stream_id);

                let payload;
                (client_buf, payload) = match read_and_parse(
                    nom::bytes::streaming::take(frame.len as usize),
                    transport.as_ref(),
                    client_buf,
                    frame.len as usize,
                )
                .await?
                {
                    Some((client_buf, frame)) => (client_buf, frame),
                    None => {
                        debug!("h2 client closed connection while reading settings body");
                        return Ok(());
                    }
                };

                let mut path: Option<PieceStr> = None;
                let mut method: Option<Method> = None;

                hpack_dec
                    .decode_with_cb(&payload[..], |key, value| {
                        debug!(
                            "got key: {:?}\nvalue: {:?}",
                            key.hex_dump(),
                            value.hex_dump()
                        );

                        if &key[..1] == b":" {
                            // it's a pseudo-header!
                            match &key[1..] {
                                b"path" => {
                                    let value: PieceStr =
                                        Piece::from(value.to_vec()).to_string().unwrap();
                                    path = Some(value);
                                }
                                b"method" => {
                                    // TODO: error handling
                                    let value: PieceStr =
                                        Piece::from(value.to_vec()).to_string().unwrap();
                                    method = Some(Method::try_from(value).unwrap());
                                }
                                other => {
                                    debug!("ignoring pseudo-header {:?}", other.hex_dump());
                                }
                            }
                        } else {
                            // TODO: what do we do in case of malformed header names?
                            // ignore it? return a 400?
                            let name =
                                HeaderName::from_bytes(&key[..]).expect("malformed header name");
                            let value: Piece = value.to_vec().into();
                            headers.append(name, value);
                        }
                    })
                    .map_err(|e| eyre::eyre!("hpack error: {e:?}"))?;

                // TODO: proper error handling (return 400)
                let path = path.unwrap();
                let method = method.unwrap();

                let req = Request {
                    method,
                    path,
                    version: Version::HTTP_2,
                    headers,
                };

                todo!("Call ServerDriver with req {req:?}");

                if !flags.contains(HeadersFlags::EndHeaders) {
                    todo!("handle continuation frames");
                }

                if flags.contains(HeadersFlags::EndStream) {
                    todo!("handle requests with no request body");
                }

                // at this point we've fully read the headers, we can start
                // reading the body.
            }
            FrameType::Priority => todo!(),
            FrameType::RstStream => todo!(),
            FrameType::Settings(s) => {
                let _payload;
                (client_buf, _payload) = match read_and_parse(
                    nom::bytes::streaming::take(frame.len as usize),
                    transport.as_ref(),
                    client_buf,
                    frame.len as usize,
                )
                .await?
                {
                    Some((client_buf, frame)) => (client_buf, frame),
                    None => {
                        debug!("h2 client closed connection while reading settings body");
                        return Ok(());
                    }
                };

                if !s.contains(SettingsFlags::Ack) {
                    // TODO: actually apply settings

                    debug!("acknowledging new settings");
                    let res_frame = Frame::new(
                        FrameType::Settings(SettingsFlags::Ack.into()),
                        StreamId::CONNECTION,
                    );
                    res_frame.write(transport.as_ref()).await?;
                }
            }
            FrameType::PushPromise => todo!("push promise not implemented"),
            FrameType::Ping => todo!(),
            FrameType::GoAway => todo!(),
            FrameType::WindowUpdate => {
                debug!("ignoring window update");

                let _payload;
                (client_buf, _payload) = match read_and_parse(
                    nom::bytes::streaming::take(frame.len as usize),
                    transport.as_ref(),
                    client_buf,
                    frame.len as usize,
                )
                .await?
                {
                    Some((client_buf, frame)) => (client_buf, frame),
                    None => {
                        debug!("h2 client closed connection while reading window update body");
                        return Ok(());
                    }
                };
            }
            FrameType::Continuation => todo!(),
        }
    }
}