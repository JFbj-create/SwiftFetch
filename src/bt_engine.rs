//! SwiftFetch v3 - 自研 BT 种子下载子引擎
//!
//! 简化版 BitTorrent 协议实现：
//! - Bencode 极简解析 (dict/list/int/bytes)
//! - Magnet URI + .torrent 文件解析
//! - HTTP Tracker announce
//! - Peer 握手 + Wire Protocol (Bitfield/Have/Unchoke/Interested/Request/Piece/Cancel)
//! - Piece 内部 16KB request 块

use async_trait::async_trait;
use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ReadBytesExt};
use parking_lot::{Mutex as PMutex, RwLock as PRwLock};
use rand::Rng;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use crate::modules::*;
use crate::speed_engine::*;

// ============================================================
// Bencode 极简实现
// ============================================================

#[derive(Debug, Clone)]
pub enum BenValue {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<BenValue>),
    Dict(HashMap<Vec<u8>, BenValue>),
}

pub struct BenParser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BenParser<'a> {
    pub fn new(data: &'a [u8]) -> Self { Self { data, pos: 0 } }

    pub fn parse(&mut self) -> anyhow::Result<BenValue> {
        self.parse_value()
    }

    fn parse_value(&mut self) -> anyhow::Result<BenValue> {
        if self.pos >= self.data.len() {
            anyhow::bail!("bencode unexpected eof");
        }
        let c = self.data[self.pos];
        match c {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            _ => anyhow::bail!("bencode invalid byte: {}", c),
        }
    }

    fn parse_int(&mut self) -> anyhow::Result<BenValue> {
        self.pos += 1;
        let end = self.find_byte(b'e')?;
        let s = std::str::from_utf8(&self.data[self.pos..end])?;
        let v: i64 = s.parse()?;
        self.pos = end + 1;
        Ok(BenValue::Int(v))
    }

    fn parse_bytes(&mut self) -> anyhow::Result<BenValue> {
        let colon = self.find_byte(b':')?;
        let s = std::str::from_utf8(&self.data[self.pos..colon])?;
        let len: usize = s.parse()?;
        let start = colon + 1;
        let end = start + len;
        if end > self.data.len() { anyhow::bail!("bencode bytes overflow"); }
        let v = self.data[start..end].to_vec();
        self.pos = end;
        Ok(BenValue::Bytes(v))
    }

    fn parse_list(&mut self) -> anyhow::Result<BenValue> {
        self.pos += 1;
        let mut list = Vec::new();
        while self.pos < self.data.len() && self.data[self.pos] != b'e' {
            list.push(self.parse_value()?);
        }
        if self.pos >= self.data.len() { anyhow::bail!("bencode list unclosed"); }
        self.pos += 1;
        Ok(BenValue::List(list))
    }

    fn parse_dict(&mut self) -> anyhow::Result<BenValue> {
        self.pos += 1;
        let mut dict = HashMap::new();
        while self.pos < self.data.len() && self.data[self.pos] != b'e' {
            let key = match self.parse_value()? {
                BenValue::Bytes(b) => b,
                _ => anyhow::bail!("bencode dict key must be bytes"),
            };
            let val = self.parse_value()?;
            dict.insert(key, val);
        }
        if self.pos >= self.data.len() { anyhow::bail!("bencode dict unclosed"); }
        self.pos += 1;
        Ok(BenValue::Dict(dict))
    }

    fn find_byte(&self, b: u8) -> anyhow::Result<usize> {
        for i in self.pos..self.data.len() {
            if self.data[i] == b { return Ok(i); }
        }
        anyhow::bail!("bencode missing terminator")
    }
}

impl BenValue {
    pub fn as_dict(&self) -> Option<&HashMap<Vec<u8>, BenValue>> {
        if let BenValue::Dict(d) = self { Some(d) } else { None }
    }
    pub fn as_list(&self) -> Option<&Vec<BenValue>> {
        if let BenValue::List(l) = self { Some(l) } else { None }
    }
    pub fn as_int(&self) -> Option<i64> {
        if let BenValue::Int(i) = self { Some(*i) } else { None }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let BenValue::Bytes(b) = self { Some(b) } else { None }
    }
    pub fn dict_get(&self, key: &str) -> Option<&BenValue> {
        self.as_dict()?.get(key.as_bytes())
    }
}

// ============================================================
// TorrentMeta: magnet + .torrent 解析
// ============================================================

#[derive(Debug, Clone)]
pub struct TorrentFileInfo {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct TorrentMeta {
    pub info_hash: [u8; 20],
    pub piece_size: u64,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<TorrentFileInfo>,
    pub total_size: u64,
    pub trackers: Vec<String>,
    pub display_name: String,
}

impl TorrentMeta {
    pub fn from_magnet(magnet: &str) -> anyhow::Result<Self> {
        let uri = url::Url::parse(magnet)?;
        let mut xt = None;
        let mut dn = None;
        let mut trs = Vec::new();
        for (k, v) in uri.query_pairs() {
            match k.as_ref() {
                "xt" => xt = Some(v.into_owned()),
                "dn" => dn = Some(v.into_owned()),
                "tr" => trs.push(v.into_owned()),
                _ => {}
            }
        }
        let xt = xt.ok_or_else(|| anyhow!("magnet missing xt"))?;
        let ih_hex = xt.strip_prefix("urn:btih:")
            .or_else(|| xt.strip_prefix("urn:btih:"))
            .ok_or_else(|| anyhow!("magnet xt format invalid"))?;
        if ih_hex.len() != 40 {
            anyhow::bail!("info_hash length invalid");
        }
        let mut info_hash = [0u8; 20];
        for i in 0..20 {
            info_hash[i] = u8::from_str_radix(&ih_hex[i*2..i*2+2], 16)?;
        }
        let name = dn.unwrap_or_else(|| "magnet-download".into());
        Ok(Self {
            info_hash,
            piece_size: 256 * 1024,
            pieces: Vec::new(),
            files: vec![TorrentFileInfo { name: name.clone(), size: 0 }],
            total_size: 0,
            trackers: trs,
            display_name: name,
        })
    }

    pub fn from_torrent_bytes(data: &[u8]) -> anyhow::Result<Self> {
        let mut parser = BenParser::new(data);
        let root = parser.parse()?;
        let dict = root.as_dict().ok_or_else(|| anyhow!(".torrent: root not dict"))?;

        let announce = dict.get(b"announce".as_ref())
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).to_string());
        let announce_list = dict.get(b"announce-list".as_ref())
            .and_then(|v| v.as_list());
        let info_val = dict.get(b"info".as_ref())
            .ok_or_else(|| anyhow!(".torrent: missing info dict"))?;

        let info_bytes = {
            let start = data.windows(b"4:name".len()).position(|w| w == b"4:name")
                .unwrap_or(data.len());
            let mut p = 0;
            let mut found = false;
            for (k, _) in root.as_dict().unwrap() {
                let key_len = k.len().to_string();
                let needle = format!("{}:{}", key_len, String::from_utf8_lossy(k));
                if let Some(pos) = data[p..].windows(needle.len()).position(|w| w == needle.as_bytes()) {
                    if k == b"info" {
                        let v_start = p + pos + needle.len();
                        let mut vp = BenParser::new(&data[v_start..]);
                        vp.parse_value()?;
                        let raw = &data[v_start..v_start + vp.pos];
                        return Self::build_from_parsed(dict, info_val, announce, announce_list, raw);
                    }
                    let mut vp = BenParser::new(&data[p + pos + needle.len()..]);
                    vp.parse_value()?;
                    p = p + pos + needle.len() + vp.pos;
                    found = true;
                }
            }
            let _ = (start, found);
            b""
        };

        Self::build_from_parsed(dict, info_val, announce, announce_list, info_bytes)
    }

    fn build_from_parsed(
        root: &HashMap<Vec<u8>, BenValue>,
        info_val: &BenValue,
        announce: Option<String>,
        announce_list: Option<&Vec<BenValue>>,
        _raw_info: &[u8],
    ) -> anyhow::Result<Self> {
        let info = info_val.as_dict().ok_or_else(|| anyhow!("info not dict"))?;
        let piece_size = info.get(b"piece length".as_ref())
            .and_then(|v| v.as_int()).ok_or_else(|| anyhow!("missing piece length"))? as u64;
        let pieces_bytes = info.get(b"pieces".as_ref())
            .and_then(|v| v.as_bytes()).ok_or_else(|| anyhow!("missing pieces"))?;
        if pieces_bytes.len() % 20 != 0 {
            anyhow::bail!("pieces length invalid");
        }
        let mut pieces = Vec::new();
        for ch in pieces_bytes.chunks(20) {
            let mut h = [0u8; 20];
            h.copy_from_slice(ch);
            pieces.push(h);
        }

        let name = info.get(b"name".as_ref())
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_else(|| "download".into());

        let mut files = Vec::new();
        let mut total_size = 0u64;
        if let Some(list) = info.get(b"files".as_ref()).and_then(|v| v.as_list()) {
            for fv in list {
                if let Some(fd) = fv.as_dict() {
                    let size = fd.get(b"length".as_ref())
                        .and_then(|v| v.as_int()).unwrap_or(0) as u64;
                    let fparts: Vec<String> = fd.get(b"path".as_ref())
                        .and_then(|v| v.as_list())
                        .map(|lp| lp.iter().filter_map(|p| p.as_bytes()
                            .map(|b| String::from_utf8_lossy(b).to_string())).collect())
                        .unwrap_or_default();
                    let fname = if fparts.is_empty() { name.clone() } else { fparts.join("/") };
                    total_size += size;
                    files.push(TorrentFileInfo { name: fname, size });
                }
            }
        } else {
            let size = info.get(b"length".as_ref())
                .and_then(|v| v.as_int()).unwrap_or(0) as u64;
            total_size = size;
            files.push(TorrentFileInfo { name: name.clone(), size });
        }

        let mut trackers = Vec::new();
        if let Some(a) = announce.clone() { trackers.push(a); }
        if let Some(al) = announce_list {
            for tier in al {
                if let Some(tl) = tier.as_list() {
                    for t in tl {
                        if let Some(tb) = t.as_bytes() {
                            trackers.push(String::from_utf8_lossy(tb).to_string());
                        }
                    }
                }
            }
        }
        trackers.dedup();

        let info_raw = dict_to_bencode(info)?;
        let mut hasher = Sha1::new();
        hasher.update(&info_raw);
        let hash = hasher.finalize();
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&hash);

        Ok(Self {
            info_hash,
            piece_size,
            pieces,
            files,
            total_size,
            trackers,
            display_name: name,
        })
    }

    pub fn aligned_base_size(&self) -> u64 {
        let mut n = 1u64;
        while n * self.piece_size < HYBRID_ALIGNED_BASE { n += 1; }
        n * self.piece_size
    }

    pub fn piece_to_base(&self, base_chunk_size: u64, piece_idx: u32) -> u32 {
        let offset = piece_idx as u64 * self.piece_size;
        (offset / base_chunk_size) as u32
    }
}

fn dict_to_bencode(dict: &HashMap<Vec<u8>, BenValue>) -> anyhow::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.push(b'd');
    let mut keys: Vec<&Vec<u8>> = dict.keys().collect();
    keys.sort_by(|a, b| a.cmp(b));
    for k in keys {
        let val = dict.get(k).unwrap();
        write_bytes_len(&mut out, k);
        write_value(&mut out, val)?;
    }
    out.push(b'e');
    Ok(out)
}

fn write_bytes_len(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(b.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(b);
}

fn write_value(out: &mut Vec<u8>, v: &BenValue) -> anyhow::Result<()> {
    match v {
        BenValue::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        BenValue::Bytes(b) => write_bytes_len(out, b),
        BenValue::List(l) => {
            out.push(b'l');
            for it in l { write_value(out, it)?; }
            out.push(b'e');
        }
        BenValue::Dict(d) => {
            let raw = dict_to_bencode(d)?;
            out.extend_from_slice(&raw);
        }
    }
    Ok(())
}

// ============================================================
// Wire Protocol 消息类型
// ============================================================

#[derive(Debug, Clone, Copy)]
pub enum BtMsgId {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
    Port = 9,
}

pub struct BtMessage;
impl BtMessage {
    pub const HANDSHAKE_PSTR: &'static [u8] = b"BitTorrent protocol";
    pub const HANDSHAKE_PSTRLEN: u8 = 19;

    pub fn build_handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Vec<u8> {
        let mut out = Vec::with_capacity(68);
        out.push(Self::HANDSHAKE_PSTRLEN);
        out.extend_from_slice(Self::HANDSHAKE_PSTR);
        out.extend_from_slice(&[0u8; 8]);
        out.extend_from_slice(info_hash);
        out.extend_from_slice(peer_id);
        out
    }

    pub fn build_interested() -> Vec<u8> {
        let mut v = Vec::with_capacity(5);
        v.extend_from_slice(&1u32.to_be_bytes());
        v.push(BtMsgId::Interested as u8);
        v
    }

    pub fn build_unchoke() -> Vec<u8> {
        let mut v = Vec::with_capacity(5);
        v.extend_from_slice(&1u32.to_be_bytes());
        v.push(BtMsgId::Unchoke as u8);
        v
    }

    pub fn build_have(piece: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(9);
        v.extend_from_slice(&5u32.to_be_bytes());
        v.push(BtMsgId::Have as u8);
        v.extend_from_slice(&piece.to_be_bytes());
        v
    }

    pub fn build_request(index: u32, begin: u32, length: u32) -> Vec<u8> {
        let mut v = Vec::with_capacity(17);
        v.extend_from_slice(&13u32.to_be_bytes());
        v.push(BtMsgId::Request as u8);
        v.extend_from_slice(&index.to_be_bytes());
        v.extend_from_slice(&begin.to_be_bytes());
        v.extend_from_slice(&length.to_be_bytes());
        v
    }

    pub fn build_bitfield(total_pieces: u32) -> Vec<u8> {
        let n_bytes = (total_pieces as usize + 7) / 8;
        let mut v = Vec::with_capacity(5 + n_bytes);
        let len = (1 + n_bytes) as u32;
        v.extend_from_slice(&len.to_be_bytes());
        v.push(BtMsgId::Bitfield as u8);
        v.extend_from_slice(&vec![0u8; n_bytes]);
        v
    }
}

// ============================================================
// 生成 peer_id
// ============================================================

pub fn generate_peer_id() -> [u8; 20] {
    let mut rng = rand::thread_rng();
    let prefix = b"-SWFT0300-";
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(&prefix[..8]);
    for i in 8..20 {
        id[i] = b"0123456789abcdef"[rng.gen_range(0..16)];
    }
    id
}

// ============================================================
// Tracker HTTP announce
// ============================================================

pub async fn tracker_announce_http(
    client: &reqwest::Client,
    tracker: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    total: u64,
    event: &str,
) -> anyhow::Result<(Vec<SocketAddr>, u32, u32)> {
    let ih_hex: String = info_hash.iter().map(|b| format!("%{:02X}", b)).collect();
    let pid_enc: String = peer_id.iter().map(|b| format!("%{:02X}", b)).collect();
    let url = format!(
        "{}?info_hash={}&peer_id={}&port={}&uploaded=0&downloaded=0&left={}&event={}&compact=1",
        tracker, ih_hex, pid_enc, port, total, event
    );
    tracing::debug!("Tracker announce: {}", &url[..url.len().min(120)]);
    let resp = client.get(&url).send().await
        .map_err(|e| anyhow!("tracker request: {}", e))?;
    if !resp.status().is_success() {
        anyhow::bail!("tracker status: {}", resp.status());
    }
    let data = resp.bytes().await
        .map_err(|e| anyhow!("tracker body: {}", e))?;
    let mut p = BenParser::new(&data);
    let root = p.parse().unwrap_or(BenValue::Dict(HashMap::new()));

    let mut peers = Vec::new();
    let mut seeders = 0u32;
    let mut leechers = 0u32;

    if let Some(d) = root.as_dict() {
        if let Some(complete) = d.get(b"complete".as_ref()).and_then(|v| v.as_int()) {
            seeders = complete.max(0) as u32;
        }
        if let Some(incomplete) = d.get(b"incomplete".as_ref()).and_then(|v| v.as_int()) {
            leechers = incomplete.max(0) as u32;
        }
        if let Some(peers_bytes) = d.get(b"peers".as_ref()).and_then(|v| v.as_bytes()) {
            for chunk in peers_bytes.chunks(6) {
                if chunk.len() == 6 {
                    let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
                    let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                    if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                        peers.push(addr);
                    }
                }
            }
        }
    }
    Ok((peers, seeders, leechers))
}

// ============================================================
// Peer Connection 管理
// ============================================================

pub struct PeerConnState {
    pub addr: SocketAddr,
    pub stream: Option<TcpStream>,
    pub peer_choked: bool,
    pub am_interested: bool,
    pub have_pieces: Vec<bool>,
    pub pending_requests: VecDeque<(u32, u32, u32)>,
    pub last_active: Instant,
    pub connected: bool,
}

impl PeerConnState {
    pub fn new(addr: SocketAddr, total_pieces: u32) -> Self {
        Self {
            addr,
            stream: None,
            peer_choked: true,
            am_interested: false,
            have_pieces: vec![false; total_pieces as usize],
            pending_requests: VecDeque::new(),
            last_active: Instant::now(),
            connected: false,
        }
    }
}

pub async fn peer_connect(
    addr: SocketAddr,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    timeout: Duration,
) -> anyhow::Result<(TcpStream, [u8; 20])> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow!("connect timeout"))??;
    let mut s = stream;
    let hs = BtMessage::build_handshake(info_hash, peer_id);
    s.write_all(&hs).await?;

    let mut pstrlen = [0u8; 1];
    tokio::time::timeout(timeout, s.read_exact(&mut pstrlen)).await
        .map_err(|_| anyhow!("handshake read timeout"))??;
    if pstrlen[0] != 19 { anyhow::bail!("invalid pstrlen"); }
    let mut pstr = [0u8; 19];
    tokio::time::timeout(timeout, s.read_exact(&mut pstr)).await??;
    if &pstr != BtMessage::HANDSHAKE_PSTR { anyhow::bail!("invalid pstr"); }
    let mut reserved = [0u8; 8];
    tokio::time::timeout(timeout, s.read_exact(&mut reserved)).await??;
    let mut ih_remote = [0u8; 20];
    tokio::time::timeout(timeout, s.read_exact(&mut ih_remote)).await??;
    if &ih_remote != info_hash { anyhow::bail!("info_hash mismatch"); }
    let mut pid_remote = [0u8; 20];
    tokio::time::timeout(timeout, s.read_exact(&mut pid_remote)).await??;

    Ok((s, pid_remote))
}

// ============================================================
// BtDownloaderModule 实现
// ============================================================

pub struct BtDownloaderModule {
    pub meta: Option<TorrentMeta>,
    pub magnet: Option<String>,
    pub torrent_file: Option<std::path::PathBuf>,
    pub peer_id: [u8; 20],
    pub port: u16,
}

impl BtDownloaderModule {
    pub fn new(
        meta: Option<TorrentMeta>,
        magnet: Option<String>,
        torrent_file: Option<std::path::PathBuf>,
        port: u16,
    ) -> Self {
        Self {
            meta,
            magnet,
            torrent_file,
            peer_id: generate_peer_id(),
            port,
        }
    }

    pub async fn resolve_meta(&mut self) -> anyhow::Result<TorrentMeta> {
        if let Some(m) = self.meta.take() { return Ok(m); }
        if let Some(p) = self.torrent_file.take() {
            let data = tokio::fs::read(&p).await
                .map_err(|e| anyhow!("read .torrent: {}", e))?;
            return TorrentMeta::from_torrent_bytes(&data);
        }
        if let Some(m) = self.magnet.take() {
            return TorrentMeta::from_magnet(&m);
        }
        anyhow::bail!("No BT source provided")
    }
}

#[async_trait]
impl DownloadModule for BtDownloaderModule {
    fn name(&self) -> &'static str { "BtDownloaderModule" }

    async fn start(self: Arc<Self>, ctx: Arc<EngineContext>) -> anyhow::Result<()> {
        if ctx.protocol == ProtocolMode::HttpOnly {
            tracing::info!("BT 模块: HttpOnly 模式, 跳过");
            return Ok(());
        }
        let mut s = Self {
            meta: self.meta.clone(),
            magnet: self.magnet.clone(),
            torrent_file: self.torrent_file.clone(),
            peer_id: self.peer_id,
            port: self.port,
        };
        let meta = match s.resolve_meta().await {
            Ok(m) => m,
            Err(e) => {
                if ctx.protocol == ProtocolMode::Hybrid {
                    tracing::warn!("BT meta 解析失败({}), Hybrid 模式降级 HTTP-only", e);
                    return Ok(());
                }
                return Err(e);
            }
        };
        ctx.bt_piece_size.store(meta.piece_size, Ordering::Relaxed);
        ctx.bt_total_pieces.store(meta.pieces.len() as u32, Ordering::Relaxed);

        if meta.total_size > 0 {
            let current = ctx.file_size.load(Ordering::Relaxed);
            if current == 0 {
                ctx.file_size.store(meta.total_size, Ordering::Relaxed);
            }
            let aligned = if meta.total_size >= MIN_BASE_SIZE_FOR_BT_ALIGN {
                let mut n = 1u64;
                while n * meta.piece_size < HYBRID_ALIGNED_BASE { n += 1; }
                n * meta.piece_size
            } else {
                ctx.base_chunk_size.load(Ordering::Relaxed).max(MIN_SUBCHUNK_SIZE * 4)
            };
            ctx.base_chunk_size.store(aligned, Ordering::Relaxed);
        }

        let client = crate::speed_engine::SwiftFetch::build_client_static(
            &ctx.config,
            ctx.network_mode == NetworkMode::FiveG,
            ctx.network_mode == NetworkMode::Wired25G,
        )?;

        let mut peers: Vec<SocketAddr> = Vec::new();
        let mut seeders = 0u32;
        let mut leechers = 0u32;
        for tr in &meta.trackers {
            match tracker_announce_http(
                &client, tr, &meta.info_hash, &self.peer_id,
                self.port, ctx.file_size.load(Ordering::Relaxed), "started"
            ).await {
                Ok((p, s, l)) => {
                    peers.extend(p);
                    seeders = seeders.max(s);
                    leechers = leechers.max(l);
                }
                Err(e) => tracing::warn!("tracker {} fail: {}", tr, e),
            }
        }
        ctx.bt_seeders.store(seeders, Ordering::Relaxed);
        ctx.bt_peers.store(leechers.saturating_add(peers.len() as u32), Ordering::Relaxed);
        peers.dedup();
        tracing::info!("BT: {} 个 peers, seeders={}, total_pieces={}", peers.len(), seeders, meta.pieces.len());

        let peer_limit = ctx.bt_peer_limit.load(Ordering::Relaxed) as usize;
        let sem = ctx.sem_bt.clone();
        let mut join_set = tokio::task::JoinSet::new();
        let meta_arc = Arc::new(meta);
        let peer_id_arc = Arc::new(self.peer_id);

        let total_pieces = ctx.bt_total_pieces.load(Ordering::Relaxed);
        for (i, addr) in peers.iter().take(peer_limit * 2).enumerate() {
            let meta_c = meta_arc.clone();
            let ctx_c = ctx.clone();
            let pid_c = peer_id_arc.clone();
            let addr = *addr;
            let sem_c = sem.clone();
            join_set.spawn(async move {
                let _permit = match sem_c.clone().try_acquire_owned() {
                    Ok(p) => Some(p),
                    Err(_) => None,
                };
                if _permit.is_none() { return; }
                tokio::time::sleep(Duration::from_millis(50 * i as u64)).await;
                ctx_c.active_bt_conns.fetch_add(1, Ordering::Relaxed);
                let result = peer_download_session(
                    addr, &meta_c, *pid_c, ctx_c.clone(), total_pieces
                ).await;
                ctx_c.active_bt_conns.fetch_sub(1, Ordering::Relaxed);
                if let Err(e) = result {
                    tracing::debug!("peer {} session: {}", addr, e);
                }
            });
        }

        let mut interval_count = 0;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    interval_count += 1;
                    let done = ctx.chunk_mgr.completed_count();
                    let total = ctx.chunk_mgr.bases.len();
                    tracing::debug!("BT tick: bases {}/{}, bt_dl={}",
                        done, total, format_bytes(ctx.bt_downloaded.load(Ordering::Relaxed)));
                    if done >= total { break; }
                    if ctx.stop_event_rx.is_disconnected() { break; }
                }
                _ = ctx.stop_notify.notified() => { break; }
                else => {
                    if join_set.is_empty() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if interval_count > 12 { break; }
                        interval_count += 1;
                    }
                    while let Some(res) = join_set.try_join_next() {
                        if let Err(e) = res {
                            tracing::debug!("peer task: {}", e);
                        }
                    }
                }
            }
        }
        join_set.shutdown().await;
        Ok(())
    }
}

async fn peer_download_session(
    addr: SocketAddr,
    meta: &TorrentMeta,
    peer_id: [u8; 20],
    ctx: Arc<EngineContext>,
    _total_pieces: u32,
) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(8);
    let (mut stream, _remote_pid) = peer_connect(addr, &meta.info_hash, &peer_id, timeout).await?;

    let total_pieces = meta.pieces.len() as u32;
    let bitfield = BtMessage::build_bitfield(total_pieces);
    stream.write_all(&bitfield).await.ok();
    stream.write_all(&BtMessage::build_interested()).await.ok();
    stream.write_all(&BtMessage::build_unchoke()).await.ok();

    let peer_addr_str = addr.to_string();
    {
        let mut scores = ctx.peer_scores.lock();
        scores.entry(peer_addr_str.clone())
            .or_insert_with(|| PeerScore::new(peer_addr_str.clone()));
    }

    let mut read_buf = vec![0u8; 64 * 1024];
    let mut write_buf: Vec<u8> = Vec::new();
    let mut pending: HashMap<(u32, u32, u32), Instant> = HashMap::new();
    let mut choked = true;
    let mut have_pieces: Vec<bool> = vec![false; total_pieces as usize];

    let session_start = Instant::now();
    let max_session = Duration::from_secs(180);

    loop {
        if session_start.elapsed() > max_session { break; }
        if pending.len() < 2 && !choked {
            let next = pick_next_piece(ctx.clone(), meta, &have_pieces);
            if let Some((idx, begin, len)) = next {
                let req = BtMessage::build_request(idx, begin, len);
                write_buf.extend_from_slice(&req);
                pending.insert((idx, begin, len), Instant::now());
            }
        }

        if !write_buf.is_empty() {
            let _ = tokio::time::timeout(Duration::from_secs(4), stream.write_all(&write_buf)).await;
            write_buf.clear();
        }

        tokio::select! {
            res = read_bt_message(&mut stream, &mut read_buf) => {
                match res {
                    Ok(msg) => {
                        handle_bt_msg(msg, ctx.clone(), meta, &mut choked, &mut have_pieces, &mut pending, addr, &peer_addr_str).await;
                    }
                    Err(e) => {
                        tracing::trace!("peer read: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let all_done = ctx.chunk_mgr.completed_count() >= ctx.chunk_mgr.bases.len();
                if all_done { break; }
            }
            _ = ctx.stop_notify.notified() => { break; }
        }

        if pending.len() > 0 {
            let now = Instant::now();
            pending.retain(|_, t| now.duration_since(*t) < Duration::from_secs(30));
        }
    }

    Ok(())
}

fn pick_next_piece(
    ctx: Arc<EngineContext>,
    meta: &TorrentMeta,
    have: &[bool],
) -> Option<(u32, u32, u32)> {
    let base_size = ctx.base_chunk_size.load(Ordering::Relaxed);
    let ordered = matches!(ctx.download_mode, DownloadMode::SequentialStream);

    let mut candidates: Vec<u32> = Vec::new();
    for i in 0..have.len() {
        if !have[i] { continue; }
        let base_idx = meta.piece_to_base(base_size, i as u32);
        if let Some(base) = ctx.chunk_mgr.bases.get(base_idx as usize) {
            if base.downloaded() >= base.size { continue; }
            let base_done = ctx.base_chunk_done.lock().contains(&base_idx);
            if base_done { continue; }
            candidates.push(i as u32);
        }
    }
    if candidates.is_empty() { return None; }

    if ordered {
        candidates.sort_unstable();
    }
    let piece_idx = candidates[0];
    let piece_done = ctx.bt_piece_map_completed.lock().contains(&piece_idx);
    if piece_done { return None; }

    let piece_size = meta.piece_size;
    let total = ctx.file_size.load(Ordering::Relaxed);
    let offset = piece_idx as u64 * piece_size;
    let remaining = if offset + piece_size > total {
        total.saturating_sub(offset)
    } else { piece_size };

    let req_block = BT_REQUEST_BLOCK;
    let begin = 0u32;
    let len = remaining.min(req_block) as u32;
    Some((piece_idx, begin, len))
}

async fn handle_bt_msg(
    msg: BtParsedMsg,
    ctx: Arc<EngineContext>,
    meta: &TorrentMeta,
    choked: &mut bool,
    have: &mut Vec<bool>,
    pending: &mut HashMap<(u32, u32, u32), Instant>,
    _addr: SocketAddr,
    addr_str: &str,
) {
    use BtMsgId::*;
    match msg.id {
        Some(Choke) => *choked = true,
        Some(Unchoke) => *choked = false,
        Some(Have) => {
            if msg.payload.len() >= 4 {
                let idx = ReadBytesExt::read_u32::<BigEndian>(&mut Cursor::new(&msg.payload)).unwrap_or(0);
                if (idx as usize) < have.len() { have[idx as usize] = true; }
            }
        }
        Some(Bitfield) => {
            for (i, byte) in msg.payload.iter().enumerate() {
                for bit in 0..8 {
                    let pidx = i * 8 + bit;
                    if pidx < have.len() {
                        have[pidx] = (byte & (1 << (7 - bit))) != 0;
                    }
                }
            }
        }
        Some(Piece) => {
            if msg.payload.len() < 8 { return; }
            let mut c = Cursor::new(&msg.payload);
            let index = ReadBytesExt::read_u32::<BigEndian>(&mut c).unwrap_or(0);
            let begin = ReadBytesExt::read_u32::<BigEndian>(&mut c).unwrap_or(0);
            let data = &msg.payload[8..];
            let data_len = data.len() as u64;
            let file_offset = index as u64 * meta.piece_size + begin as u64;
            if write_data_to_file(ctx.clone(), file_offset, data).await.is_ok() {
                let base_idx = meta.piece_to_base(ctx.base_chunk_size.load(Ordering::Relaxed), index);
                if let Some(base) = ctx.chunk_mgr.bases.get(base_idx as usize) {
                    let rel = file_offset - base.start + data_len;
                    let mut prev = base.downloaded_atomic.load(Ordering::Relaxed);
                    loop {
                        let new = prev.max(rel);
                        match base.downloaded_atomic.compare_exchange_weak(
                            prev, new, Ordering::Relaxed, Ordering::Relaxed
                        ) {
                            Ok(_) => break,
                            Err(x) => prev = x,
                        }
                    }
                    if base.downloaded() >= base.size {
                        let mut done = ctx.base_chunk_done.lock();
                        if !done.contains(&base_idx) { done.push(base_idx); }
                    }
                }
                ctx.bt_downloaded.fetch_add(data_len, Ordering::Relaxed);
                ctx.downloaded.fetch_add(data_len, Ordering::Relaxed);

                let mut scores = ctx.peer_scores.lock();
                if let Some(s) = scores.get_mut(addr_str) {
                    s.update_speed(data_len, 1.0);
                    s.pieces_sent += 1;
                }

                pending.remove(&(index, begin, data_len as u32));
            }
        }
        _ => {}
    }
}

async fn write_data_to_file(ctx: Arc<EngineContext>, offset: u64, data: &[u8]) -> anyhow::Result<()> {
    let mut f_guard = ctx.file.lock().await;
    if let Some(f) = f_guard.as_mut() {
        use tokio::io::AsyncSeekExt;
        f.seek(std::io::SeekFrom::Start(offset)).await.ok();
        f.write_all(data).await?;
    }
    Ok(())
}

// ============================================================
// BT 消息帧读取
// ============================================================

pub struct BtParsedMsg {
    pub id: Option<BtMsgId>,
    pub payload: Vec<u8>,
}

async fn read_bt_message(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> anyhow::Result<BtParsedMsg> {
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(20), stream.read_exact(&mut len_buf)).await??;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(BtParsedMsg { id: None, payload: Vec::new() });
    }
    if len > 16 * 1024 * 1024 {
        anyhow::bail!("bt msg too large: {}", len);
    }
    if buf.len() < len { buf.resize(len, 0); }
    tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut buf[..len])).await??;
    let id_byte = buf[0];
    let id = match id_byte {
        0 => Some(BtMsgId::Choke),
        1 => Some(BtMsgId::Unchoke),
        2 => Some(BtMsgId::Interested),
        3 => Some(BtMsgId::NotInterested),
        4 => Some(BtMsgId::Have),
        5 => Some(BtMsgId::Bitfield),
        6 => Some(BtMsgId::Request),
        7 => Some(BtMsgId::Piece),
        8 => Some(BtMsgId::Cancel),
        9 => Some(BtMsgId::Port),
        _ => None,
    };
    let payload = if len > 1 { buf[1..len].to_vec() } else { Vec::new() };
    Ok(BtParsedMsg { id, payload })
}
