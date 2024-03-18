//! Types for performing vectored I/O.

use std::{fmt, ops::Deref, str::Utf8Error};

use fluke_maybe_uring::buf::IoBuf;
use http::header::HeaderName;

use crate::{Roll, RollStr};

/// A piece of data (arbitrary bytes) with a stable address, suitable for
/// passing to the kernel (io_uring writes).
#[derive(Clone)]
pub enum Piece {
    Static(&'static [u8]),
    Vec(Vec<u8>),
    Roll(Roll),
    HeaderName(HeaderName),
}

impl From<&'static [u8]> for Piece {
    fn from(slice: &'static [u8]) -> Self {
        Piece::Static(slice)
    }
}

impl From<&'static str> for Piece {
    fn from(slice: &'static str) -> Self {
        Piece::Static(slice.as_bytes())
    }
}

impl From<Vec<u8>> for Piece {
    fn from(vec: Vec<u8>) -> Self {
        Piece::Vec(vec)
    }
}

impl From<Roll> for Piece {
    fn from(roll: Roll) -> Self {
        Piece::Roll(roll)
    }
}

impl From<PieceStr> for Piece {
    fn from(s: PieceStr) -> Self {
        s.piece
    }
}

impl From<HeaderName> for Piece {
    fn from(name: HeaderName) -> Self {
        Piece::HeaderName(name)
    }
}

impl Deref for Piece {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Piece {
    fn as_ref(&self) -> &[u8] {
        match self {
            Piece::Static(slice) => slice,
            Piece::Vec(vec) => vec.as_ref(),
            Piece::Roll(roll) => roll.as_ref(),
            Piece::HeaderName(name) => name.as_str().as_bytes(),
        }
    }
}

impl Piece {
    /// Decode as utf-8 (borrowed)
    pub fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.as_ref())
    }

    /// Decode as utf-8 (owned)
    pub fn to_str(self) -> Result<PieceStr, Utf8Error> {
        _ = std::str::from_utf8(&self)?;
        Ok(PieceStr { piece: self })
    }

    /// Convert to [PieceStr].
    ///
    /// # Safety
    /// UB if not utf-8. Typically only used in parsers.
    pub unsafe fn to_string_unchecked(self) -> PieceStr {
        PieceStr { piece: self }
    }
}

unsafe impl IoBuf for Piece {
    #[inline(always)]
    fn stable_ptr(&self) -> *const u8 {
        match self {
            Piece::Static(s) => IoBuf::stable_ptr(s),
            Piece::Vec(s) => IoBuf::stable_ptr(s),
            Piece::Roll(s) => IoBuf::stable_ptr(s),
            Piece::HeaderName(s) => s.as_str().as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match self {
            Piece::Static(s) => IoBuf::bytes_init(s),
            Piece::Vec(s) => IoBuf::bytes_init(s),
            Piece::Roll(s) => IoBuf::bytes_init(s),
            Piece::HeaderName(s) => s.as_str().len(),
        }
    }

    fn bytes_total(&self) -> usize {
        match self {
            Piece::Static(s) => IoBuf::bytes_total(s),
            Piece::Vec(s) => IoBuf::bytes_total(s),
            Piece::Roll(s) => IoBuf::bytes_total(s),
            Piece::HeaderName(s) => s.as_str().len(),
        }
    }
}

impl Piece {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A list of [Piece], suitable for issuing vectored writes via io_uring.
#[derive(Default)]
pub struct PieceList {
    // TODO: use smallvec?
    pieces: Vec<Piece>,
}

impl PieceList {
    /// Add a single chunk to the list
    pub fn push(&mut self, chunk: impl Into<Piece>) {
        self.pieces.push(chunk.into());
    }

    /// Add a single chunk to the list and return self
    pub fn with(mut self, chunk: impl Into<Piece>) -> Self {
        self.push(chunk);
        self
    }

    /// Returns total length
    pub fn len(&self) -> usize {
        self.pieces.iter().map(|c| c.len()).sum()
    }

    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty() || self.len() == 0
    }

    pub fn clear(&mut self) {
        self.pieces.clear();
    }

    pub fn into_vec(self) -> Vec<Piece> {
        self.pieces
    }
}

impl From<Vec<Piece>> for PieceList {
    fn from(chunks: Vec<Piece>) -> Self {
        Self { pieces: chunks }
    }
}

impl From<PieceList> for Vec<Piece> {
    fn from(list: PieceList) -> Self {
        list.pieces
    }
}

/// A piece of data with a stable address that's _also_
/// valid utf-8.
#[derive(Clone)]
pub struct PieceStr {
    piece: Piece,
}

impl fmt::Debug for PieceStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(&self[..], f)
    }
}

impl fmt::Display for PieceStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self)
    }
}

impl Deref for PieceStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe { std::str::from_utf8_unchecked(&self.piece) }
    }
}

impl AsRef<str> for PieceStr {
    fn as_ref(&self) -> &str {
        self
    }
}

impl PieceStr {
    /// Returns the underlying bytes (borrowed)
    pub fn as_bytes(&self) -> &[u8] {
        self.piece.as_ref()
    }

    /// Returns the underlying bytes (owned)
    pub fn into_inner(self) -> Piece {
        self.piece
    }
}

impl From<&'static str> for PieceStr {
    fn from(s: &'static str) -> Self {
        PieceStr {
            piece: Piece::Static(s.as_bytes()),
        }
    }
}

impl From<String> for PieceStr {
    fn from(s: String) -> Self {
        PieceStr {
            piece: Piece::Vec(s.into_bytes()),
        }
    }
}

impl From<RollStr> for PieceStr {
    fn from(s: RollStr) -> Self {
        PieceStr {
            piece: Piece::Roll(s.into_inner()),
        }
    }
}
