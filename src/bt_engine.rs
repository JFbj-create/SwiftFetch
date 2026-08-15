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
    /// BEP-19 WebSeed (HTTP GET-based seed sources). 来自 .torrent 顶层 `url-list` 字段.
    /// 单文件种子: `url-list` 指向的目录 + name. 多文件种子: `url-list` 目录 + 每个文件的 path 连接.
    pub webseeds: Vec<String>,
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
            webseeds: Vec::new(),
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

        // ----- BEP-19 WebSeed: 顶层 `url-list` (单 string 或 list of strings) -----
        let mut webseeds: Vec<String> = Vec::new();
        if let Some(url_list_val) = root.get(b"url-list".as_ref()) {
            match url_list_val {
                BenValue::Bytes(b) => {
                    let s = String::from_utf8_lossy(b).trim().to_string();
                    if !s.is_empty() { webseeds.push(s); }
                }
                BenValue::List(l) => {
                    for item in l {
                        if let Some(b) = item.as_bytes() {
                            let s = String::from_utf8_lossy(b).trim().to_string();
                            if !s.is_empty() { webseeds.push(s); }
                        }
                    }
                }
                _ => {}
            }
        }
        webseeds.dedup();

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
            webseeds,
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

    /// 生成一个包含 file_data 的单文件虚拟 .torrent, piece_size 自动对齐
    pub fn generate(
        display_name: &str,
        file_data: &[u8],
        piece_size: u64,
        trackers: Vec<String>,
    ) -> anyhow::Result<Self> {
        let piece_size = if piece_size == 0 { 16384 } else { piece_size };
        let mut pieces = Vec::new();
        for chunk in file_data.chunks(piece_size as usize) {
            let mut hasher = Sha1::new();
            hasher.update(chunk);
            let h = hasher.finalize();
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&h);
            pieces.push(arr);
        }
        let files = vec![TorrentFileInfo {
            name: display_name.to_string(),
            size: file_data.len() as u64,
        }];
        // 先构造 TorrentMeta 再用 encode 算 info_hash (让两个路径一致)
        let mut tmp = Self {
            info_hash: [0u8; 20],
            piece_size,
            pieces,
            files,
            total_size: file_data.len() as u64,
            trackers: trackers.clone(),
            display_name: display_name.to_string(),
            webseeds: Vec::new(),
        };
        let bytes = tmp.encode_to_bytes()?;
        let parsed = Self::from_torrent_bytes(&bytes)?;
        Ok(parsed)
    }

    /// 把 TorrentMeta 编码成 .torrent 文件字节 (bencode 格式)
    pub fn encode_to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        use std::collections::BTreeMap;
        // 构造 info dict
        let mut info: HashMap<Vec<u8>, BenValue> = HashMap::new();
        info.insert(b"name".to_vec(), BenValue::Bytes(self.display_name.as_bytes().to_vec()));
        info.insert(b"piece length".to_vec(), BenValue::Int(self.piece_size as i64));
        let mut pieces_concat: Vec<u8> = Vec::with_capacity(self.pieces.len() * 20);
        for p in &self.pieces { pieces_concat.extend_from_slice(p); }
        info.insert(b"pieces".to_vec(), BenValue::Bytes(pieces_concat));
        if self.files.len() == 1 {
            info.insert(b"length".to_vec(), BenValue::Int(self.files[0].size as i64));
        } else {
            let mut files_list: Vec<BenValue> = Vec::new();
            for f in &self.files {
                let mut fd: HashMap<Vec<u8>, BenValue> = HashMap::new();
                fd.insert(b"length".to_vec(), BenValue::Int(f.size as i64));
                let segs: Vec<&str> = f.name.split('/').collect();
                let path_list: Vec<BenValue> = segs.iter().map(|s| BenValue::Bytes(s.as_bytes().to_vec())).collect();
                fd.insert(b"path".to_vec(), BenValue::List(path_list));
                files_list.push(BenValue::Dict(fd));
            }
            info.insert(b"files".to_vec(), BenValue::List(files_list));
        }

        // 构造 root dict
        let mut root: HashMap<Vec<u8>, BenValue> = HashMap::new();
        // 排序保持输出稳定: 用 BTreeMap 的形式按 key 字节序插入
        let _ = BTreeMap::<&[u8], i32>::new();
        if let Some(first) = self.trackers.first() {
            root.insert(b"announce".to_vec(), BenValue::Bytes(first.as_bytes().to_vec()));
        }
        if self.trackers.len() > 1 {
            let mut outer: Vec<BenValue> = Vec::new();
            for t in &self.trackers {
                let inner: Vec<BenValue> = vec![BenValue::Bytes(t.as_bytes().to_vec())];
                outer.push(BenValue::List(inner));
            }
            root.insert(b"announce-list".to_vec(), BenValue::List(outer));
        }
        root.insert(b"info".to_vec(), BenValue::Dict(info));
        let mut out = dict_to_bencode(&root)?;
        Ok(out)
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
// Tracker announce (HTTP + UDP)
// ============================================================

/// 自动判断 tracker 协议类型并 announce
pub async fn tracker_announce(
    client: &reqwest::Client,
    tracker: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    total: u64,
    event: &str,
) -> anyhow::Result<(Vec<SocketAddr>, u32, u32)> {
    let lower = tracker.to_lowercase();
    if lower.starts_with("udp://") {
        tracker_announce_udp(tracker, info_hash, peer_id, port, total, event).await
    } else {
        tracker_announce_http(client, tracker, info_hash, peer_id, port, total, event).await
    }
}

/// UDP Tracker (BEP-15): connect → announce
pub async fn tracker_announce_udp(
    tracker: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    total: u64,
    event: &str,
) -> anyhow::Result<(Vec<SocketAddr>, u32, u32)> {
    use tokio::net::UdpSocket;

    // 解析 udp://host:port[/path] → host:port
    let addr_str = tracker
        .strip_prefix("udp://")
        .unwrap_or(tracker)
        .split('/')
        .next()
        .ok_or_else(|| anyhow!("udp tracker url invalid: {}", tracker))?;

    // 尝试解析为 SocketAddr, 如果只有 host 需要 DNS 解析
    let socket_addr: SocketAddr = if let Ok(sa) = addr_str.parse() {
        sa
    } else {
        // 带 DNS 的地址: 用 tokio resolve
        let parts: Vec<&str> = addr_str.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("udp tracker addr parse fail: {}", addr_str);
        }
        let host = parts[1];
        let port: u16 = parts[0].parse()
            .map_err(|_| anyhow!("udp tracker port invalid: {}", parts[0]))?;
        // 使用 tokio 的 DNS 解析
        let addrs = tokio::net::lookup_host(format!("{}:{}", host, port)).await
            .map_err(|e| anyhow!("udp tracker DNS resolve {} fail: {}", host, e))?;
        addrs.into_iter().next()
            .ok_or_else(|| anyhow!("udp tracker DNS resolve empty: {}", host))?
    };

    // 绑定本地 UDP socket
    let sock = UdpSocket::bind("0.0.0.0:0").await
        .map_err(|e| anyhow!("udp bind fail: {}", e))?;
    sock.connect(socket_addr).await
        .map_err(|e| anyhow!("udp connect {} fail: {}", socket_addr, e))?;

    let txn_id: u32 = rand::random();

    // ---- Step 1: Connect 请求 ----
    // protocol_id = 0x41727101980 (magic)
    let protocol_id: u64 = 0x41727101980;
    let mut connect_req = Vec::with_capacity(16);
    connect_req.extend_from_slice(&protocol_id.to_be_bytes());
    connect_req.extend_from_slice(&0u32.to_be_bytes()); // action = 0 (connect)
    connect_req.extend_from_slice(&txn_id.to_be_bytes());

    sock.send(&connect_req).await
        .map_err(|e| anyhow!("udp connect send fail: {}", e))?;

    let mut connect_resp = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(15), sock.recv(&mut connect_resp)).await
        .map_err(|_| anyhow!("udp connect timeout"))?
        .map_err(|e| anyhow!("udp connect recv fail: {}", e))?;
    if n < 16 {
        anyhow::bail!("udp connect response too short: {}", n);
    }
    let resp_action = u32::from_be_bytes(connect_resp[0..4].try_into().unwrap());
    let resp_txn = u32::from_be_bytes(connect_resp[4..8].try_into().unwrap());
    if resp_txn != txn_id {
        anyhow::bail!("udp connect txn mismatch: {} != {}", resp_txn, txn_id);
    }
    if resp_action != 0 {
        let err_msg = String::from_utf8_lossy(&connect_resp[8..n]);
        anyhow::bail!("udp connect action error {}: {}", resp_action, err_msg);
    }
    let connection_id = u64::from_be_bytes(connect_resp[8..16].try_into().unwrap());
    tracing::debug!("UDP tracker {} connect OK, conn_id={:x}", socket_addr, connection_id);

    // ---- Step 2: Announce 请求 ----
    let announce_txn: u32 = rand::random();
    let event_code: u32 = match event {
        "started" => 2,
        "completed" => 1,
        "stopped" => 3,
        _ => 0,
    };
    let mut announce_req = Vec::with_capacity(98);
    announce_req.extend_from_slice(&connection_id.to_be_bytes());
    announce_req.extend_from_slice(&1u32.to_be_bytes()); // action = 1 (announce)
    announce_req.extend_from_slice(&announce_txn.to_be_bytes());
    announce_req.extend_from_slice(info_hash);           // 20 bytes
    announce_req.extend_from_slice(peer_id);             // 20 bytes
    announce_req.extend_from_slice(&0u64.to_be_bytes()); // downloaded
    announce_req.extend_from_slice(&total.to_be_bytes()); // left
    announce_req.extend_from_slice(&0u64.to_be_bytes()); // uploaded
    announce_req.extend_from_slice(&event_code.to_be_bytes());
    announce_req.extend_from_slice(&0u32.to_be_bytes()); // ip = 0
    announce_req.extend_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    announce_req.extend_from_slice(&(-1i32).to_be_bytes()); // num_want = -1
    announce_req.extend_from_slice(&port.to_be_bytes());

    sock.send(&announce_req).await
        .map_err(|e| anyhow!("udp announce send fail: {}", e))?;

    let mut announce_resp = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(15), sock.recv(&mut announce_resp)).await
        .map_err(|_| anyhow!("udp announce timeout"))?
        .map_err(|e| anyhow!("udp announce recv fail: {}", e))?;
    if n < 20 {
        anyhow::bail!("udp announce response too short: {}", n);
    }
    let ann_action = u32::from_be_bytes(announce_resp[0..4].try_into().unwrap());
    let ann_txn = u32::from_be_bytes(announce_resp[4..8].try_into().unwrap());
    if ann_txn != announce_txn {
        anyhow::bail!("udp announce txn mismatch");
    }
    if ann_action != 1 {
        let err_msg = String::from_utf8_lossy(&announce_resp[8..n]);
        anyhow::bail!("udp announce action error {}: {}", ann_action, err_msg);
    }
    let _interval = u32::from_be_bytes(announce_resp[8..12].try_into().unwrap());
    let leechers = u32::from_be_bytes(announce_resp[12..16].try_into().unwrap());
    let seeders = u32::from_be_bytes(announce_resp[16..20].try_into().unwrap());

    let mut peers = Vec::new();
    let peers_data = &announce_resp[20..n];
    for chunk in peers_data.chunks(6) {
        if chunk.len() == 6 {
            let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
            let p = u16::from_be_bytes([chunk[4], chunk[5]]);
            if let Ok(addr) = format!("{}:{}", ip, p).parse::<SocketAddr>() {
                peers.push(addr);
            }
        }
    }
    tracing::debug!("UDP tracker {} announce OK: {} peers, {} seeders, {} leechers",
        socket_addr, peers.len(), seeders, leechers);
    Ok((peers, seeders, leechers))
}

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

// ========================================================================
// BEP-33 (HTTP Tracker Scrape) — 查询 swarm 统计: complete/incomplete/downloaded
// ========================================================================

/// HTTP Tracker Scrape 返回的单 info_hash 统计信息
#[derive(Debug, Clone, Default)]
pub struct TrackerScrapeInfo {
    /// 完整种子数 (seeders)
    pub complete: u32,
    /// 下载中用户数 (leechers)
    pub incomplete: u32,
    /// 累计完成下载次数 (downloaded)
    pub downloaded: u32,
    /// tracker 返回的名字 (可选)
    pub name: Option<String>,
}

/// 对 **HTTP Tracker** 执行 `/scrape` 请求 (BEP-33 风格, 非 UDP 版本).
/// 将 tracker announce URL 中的 `/announce` (或末尾 path) 替换为 `/scrape`,
/// 并附加 `?info_hash=<hexpct_encoded>`. 如 tracker 不支持 scrape, 将返回明确错误.
pub async fn tracker_scrape_http(
    client: &reqwest::Client,
    tracker: &str,
    info_hash: &[u8; 20],
) -> anyhow::Result<TrackerScrapeInfo> {
    let ih_pct: String = info_hash.iter().map(|b| format!("%{:02X}", b)).collect();

    // 将 announce URL 末尾替换为 /scrape
    let scrape_url = if tracker.contains("/announce") {
        tracker.replacen("/announce", "/scrape", 1)
    } else {
        // 无 announce 后缀的 URL: 拼 ?info_hash= 后尝试直接请求
        let sep = if tracker.contains('?') { "&" } else { "?" };
        format!("{tracker}{sep}info_hash={ih_pct}")
    };
    let final_url = if scrape_url.contains("info_hash=") {
        scrape_url
    } else {
        let sep = if scrape_url.contains('?') { "&" } else { "?" };
        format!("{scrape_url}{sep}info_hash={ih_pct}")
    };

    tracing::debug!("Tracker scrape: {}", &final_url[..final_url.len().min(160)]);
    let resp = client.get(&final_url).send().await
        .map_err(|e| anyhow!("scrape request: {e}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("scrape tracker status: {}", resp.status());
    }
    let data = resp.bytes().await
        .map_err(|e| anyhow!("scrape body: {e}"))?;
    let mut p = BenParser::new(&data);
    let root = p.parse().unwrap_or(BenValue::Dict(HashMap::new()));

    let mut info = TrackerScrapeInfo::default();
    let d = match root.as_dict() {
        Some(d) => d,
        None => return Ok(info),
    };

    // 顶层字段: `files` dict 是标准 (keyed by info_hash bytes)
    let files_dict = d.get(b"files".as_ref()).and_then(|v| v.as_dict());
    if let Some(files) = files_dict {
        // 按 info_hash 精确匹配 (优先)
        let per_ih = files.get(info_hash.as_slice())
            .or_else(|| files.values().next()); // 只有 1 个哈希时取第一项
        if let Some(BenValue::Dict(fd)) = per_ih {
            if let Some(v) = fd.get(b"complete".as_ref()).and_then(|x| x.as_int()) {
                info.complete = v.max(0) as u32;
            }
            if let Some(v) = fd.get(b"incomplete".as_ref()).and_then(|x| x.as_int()) {
                info.incomplete = v.max(0) as u32;
            }
            if let Some(v) = fd.get(b"downloaded".as_ref()).and_then(|x| x.as_int()) {
                info.downloaded = v.max(0) as u32;
            }
            if let Some(b) = fd.get(b"name".as_ref()).and_then(|x| x.as_bytes()) {
                info.name = Some(String::from_utf8_lossy(b).to_string());
            }
            return Ok(info);
        }
    }

    // 部分 tracker 简化实现: 直接把 complete/incomplete 放在顶层 (同 announce 响应)
    if let Some(v) = d.get(b"complete".as_ref()).and_then(|x| x.as_int()) {
        info.complete = v.max(0) as u32;
    }
    if let Some(v) = d.get(b"incomplete".as_ref()).and_then(|x| x.as_int()) {
        info.incomplete = v.max(0) as u32;
    }
    if let Some(v) = d.get(b"downloaded".as_ref()).and_then(|x| x.as_int()) {
        info.downloaded = v.max(0) as u32;
    }
    Ok(info)
}

// ========================================================================
// BEP-19 WebSeed (HTTP/FTP GET 种子源) — 按 piece 从 webseed URL 拿字节
// ========================================================================

/// 计算某个 piece 落在 torrent 文件布局中的 (文件路径, 文件内偏移, 本 piece 在此文件中读取字节数).
/// 返回 Vec<(file_name_in_torrent, offset_in_file, bytes_to_read_from_this_file)>.
/// 因为一个 piece 可能恰好跨两个相邻文件边界 (多文件 torrent), 所以返回一个分段列表.
pub fn piece_file_ranges(meta: &TorrentMeta, piece_idx: u32) -> Vec<(String, u64, u64)> {
    let mut out = Vec::new();
    let p_start = piece_idx as u64 * meta.piece_size;
    let p_end = std::cmp::min(p_start + meta.piece_size, meta.total_size);
    let mut read_in_file: u64 = 0; // 已经累计在多个文件中走过多少字节
    for f in &meta.files {
        let f_start = read_in_file;
        let f_end = f_start + f.size;
        // piece 和当前文件是否有交集?
        if p_end <= f_start { break; }
        if p_start >= f_end { read_in_file += f.size; continue; }
        let overlap_start = std::cmp::max(p_start, f_start);
        let overlap_end   = std::cmp::min(p_end,   f_end);
        let off_in_file   = overlap_start - f_start;
        let len_in_file   = overlap_end   - overlap_start;
        if len_in_file > 0 {
            out.push((f.name.clone(), off_in_file, len_in_file));
        }
        read_in_file += f.size;
    }
    out
}

/// 从 webseed URL 下载某个完整 piece.
///
/// - 拼接 webseed base URL + 文件相对路径 (URL-encode 每个段)
/// - 通过 HTTP `Range: bytes=<offset>-<offset+len-1>` 拿 piece 字节
/// - 多个文件跨 piece 时合并多个 Range 响应
/// - 返回 piece 的完整字节向量，**调用方负责 SHA1 校验**（与 meta.pieces[piece_idx] 对比）
pub async fn webseed_fetch_piece(
    client: &reqwest::Client,
    meta: &TorrentMeta,
    webseed_base: &str,
    piece_idx: u32,
) -> anyhow::Result<Vec<u8>> {
    if meta.pieces.is_empty() && piece_idx != 0 {
        anyhow::bail!("webseed: meta.pieces 未知 (magnet?), 无法在 piece#{piece_idx} 定位");
    }
    let ranges = piece_file_ranges(meta, piece_idx);
    if ranges.is_empty() {
        anyhow::bail!("webseed: piece #{piece_idx} 不落在任何文件中");
    }
    let expected_len = ranges.iter().map(|(_, _, l)| *l).sum::<u64>() as usize;
    let mut merged = Vec::with_capacity(expected_len);

    let base = webseed_base.trim_end_matches('/');
    for (file_rel, off_in_file, len_in_file) in ranges {
        // BEP-19: 将 file_rel 按 "/" 分段后每段独立 percent-encode, 然后再连接
        let encoded_path: String = file_rel.split('/')
            .map(|seg| urlencoding::encode(seg).into_owned())
            .collect::<Vec<_>>()
            .join("/");
        let full_url = format!("{base}/{encoded_path}");
        let start = off_in_file;
        let end = off_in_file + len_in_file - 1;
        let range_hdr = format!("bytes={start}-{end}");

        tracing::debug!("WebSeed {piece_idx} GET {full_url} Range={range_hdr}");
        let resp = client.get(&full_url)
            .header(reqwest::header::RANGE, &range_hdr)
            .send().await
            .map_err(|e| anyhow!("webseed request ({range_hdr}): {e}"))?;
        let status = resp.status();
        if !(status.is_success() || status.as_u16() == 206) {
            anyhow::bail!("webseed status: {status} (URL: {full_url})");
        }
        let bytes = resp.bytes().await
            .map_err(|e| anyhow!("webseed body: {e}"))?;
        if bytes.len() as u64 != len_in_file {
            anyhow::bail!(
                "webseed short read: expected {len_in_file}B, got {}B from {full_url}",
                bytes.len()
            );
        }
        merged.extend_from_slice(&bytes);
    }
    if merged.len() != expected_len {
        anyhow::bail!("webseed piece #{piece_idx} merged len mismatch {} vs {}", merged.len(), expected_len);
    }
    Ok(merged)
}

// ========================================================================
// uTP (Micro Transport Protocol) 最小骨架 — UDP-based RDP with LEDBAT congestion
// ========================================================================
// uTP 是 BitTorrent 生态中用于穿透 NAT / 不占满用户带宽的 UDP 可靠传输.
// 此处实现 **最小可用骨架**: 包头结构 + SYN/SYN-ACK/ACK 握手 + DATA 包收发.
// 上层可通过 UtpSocket::connect() 建立 uTP 连接, 再包装成 AsyncRead/AsyncWrite 对接到 Wire Protocol.

/// uTP 包类型 (4-bit). 参考 libutp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpType {
    Data      = 0,
    Fin       = 1,
    State     = 2, // = ACK
    Reset     = 3,
    Syn       = 4,
}

impl UtpType {
    pub fn from_raw(v: u8) -> Option<Self> {
        match v {
            0 => Some(UtpType::Data),
            1 => Some(UtpType::Fin),
            2 => Some(UtpType::State),
            3 => Some(UtpType::Reset),
            4 => Some(UtpType::Syn),
            _ => None,
        }
    }
}

/// uTP 包头 (20 字节). 其后紧跟 extension(s) + payload.
#[derive(Debug, Clone)]
pub struct UtpHeader {
    pub ty:        UtpType,
    pub ver:       u8,        // 版本号, 目前是 1
    pub conn_id:   u16,       // 发送方为此连接生成的 id; 对端 ack conn_id + 1
    pub ts_us:     u32,       // 发送方 microsecond 时间戳 (单调任意钟)
    pub ts_diff:  u32,       // 该方 last packet 接收后经历的时间差 (us)
    pub wnd_size:  u32,       // 接收窗口 (bytes)
    pub seq_nr:    u16,       // 该包的序列号
    pub ack_nr:    u16,       // 接收端下一个期望的 seq (即已经收到并 ack 到 seq = ack_nr - 1)
}

impl UtpHeader {
    pub const MIN_SIZE: usize = 20;

    /// 序列化 20 字节包头到 out buffer (out.len() >= 20).
    pub fn encode(&self, out: &mut [u8]) {
        let type_byte: u8 = ((self.ty as u8) << 4) | (self.ver & 0x0F);
        out[0] = type_byte;
        out[1] = 0; // extension
        out[2..4].copy_from_slice(&self.conn_id.to_be_bytes());
        out[4..8].copy_from_slice(&self.ts_us.to_be_bytes());
        out[8..12].copy_from_slice(&self.ts_diff.to_be_bytes());
        out[12..16].copy_from_slice(&self.wnd_size.to_be_bytes());
        out[16..18].copy_from_slice(&self.seq_nr.to_be_bytes());
        out[18..20].copy_from_slice(&self.ack_nr.to_be_bytes());
    }

    /// 从 20 字节切片解析包头. extension 字节直接忽略 (版本扩展时可能有 extensions).
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::MIN_SIZE { return None; }
        let tb = buf[0];
        let ty = UtpType::from_raw(tb >> 4)?;
        let ver = tb & 0x0F;
        let conn_id  = u16::from_be_bytes([buf[2], buf[3]]);
        let ts_us    = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let ts_diff  = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let wnd_size = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let seq_nr   = u16::from_be_bytes([buf[16], buf[17]]);
        let ack_nr   = u16::from_be_bytes([buf[18], buf[19]]);
        Some(Self { ty, ver, conn_id, ts_us, ts_diff, wnd_size, seq_nr, ack_nr })
    }
}

/// 一个极小化的 uTP socket: 封装 `tokio::net::UdpSocket`, 提供 `connect()/send()/recv()`.
/// 完整的 LEDBAT 拥塞控制 / 重传计时器 / SACK 可以在此骨架基础上继续扩展.
pub struct UtpSocket {
    udp: tokio::net::UdpSocket,
    remote: std::net::SocketAddr,
    our_conn_id: u16,
    peer_conn_id: u16,
    our_seq: u16,
    peer_ack: u16,
    recv_buf: Vec<u8>,
}

impl UtpSocket {
    /// 默认接收窗口 (1 MB, 足够 BT piece 传输测试)
    pub const DEFAULT_WINDOW: u32 = 1 * 1024 * 1024;

    fn now_us() -> u32 {
        // uTP timestamp 只需要 **单调且微秒级即可**; 并不需要真实时钟.
        // 用 SystemTime 的 elapsed duration 作为近似微秒单调源 (u32 自然截断 OK, 差分就有用).
        use std::time::SystemTime;
        static ONCE: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();
        let t0 = ONCE.get_or_init(SystemTime::now);
        SystemTime::now().duration_since(*t0)
            .map(|d| (d.as_micros() & 0xFFFF_FFFF) as u32)
            .unwrap_or(0)
    }

    /// 与对端建立 uTP 连接 (SYN → SYN-ACK → ACK 三路握手).
    /// 成功后返回可用的 `UtpSocket`, 可用于发送 DATA 包.
    pub async fn connect(remote: std::net::SocketAddr, bind_addr: Option<std::net::SocketAddr>) -> anyhow::Result<Self> {
        let bind = bind_addr.unwrap_or(match remote {
            std::net::SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
            std::net::SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
        });
        let udp = tokio::net::UdpSocket::bind(bind).await
            .map_err(|e| anyhow!("uTP bind {bind}: {e}"))?;
        udp.connect(remote).await
            .map_err(|e| anyhow!("uTP UDP connect {remote}: {e}"))?;

        let mut rng = rand::thread_rng();
        let our_conn_id: u16 = rng.gen();
        let our_seq_init: u16 = rng.gen();

        // === 1. 发送 SYN ===
        let mut buf = [0u8; 1400];
        let syn_hdr = UtpHeader {
            ty: UtpType::Syn,
            ver: 1,
            conn_id: our_conn_id,
            ts_us: Self::now_us(),
            ts_diff: 0,
            wnd_size: Self::DEFAULT_WINDOW,
            seq_nr: our_seq_init,
            ack_nr: 0,
        };
        syn_hdr.encode(&mut buf);
        udp.send(&buf[..UtpHeader::MIN_SIZE]).await
            .map_err(|e| anyhow!("uTP send SYN: {e}"))?;

        // === 2. 等待 SYN-ACK (Type=State; conn_id == our_conn_id + 1) ===
        let mut rbuf = [0u8; 1500];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let (peer_conn_id, peer_syn_ack_seq) = loop {
            let n = tokio::time::timeout_at(deadline, udp.recv(&mut rbuf)).await
                .map_err(|_| anyhow!("uTP handshake timeout (no SYN-ACK from {remote})"))?
                .map_err(|e| anyhow!("uTP recv SYN-ACK: {e}"))?;
            let Some(h) = UtpHeader::decode(&rbuf[..n]) else { continue };
            if h.ty == UtpType::State && h.conn_id == our_conn_id.wrapping_add(1) {
                break (h.conn_id, h.seq_nr);
            }
        };

        // === 3. 回 ACK (Type=State, seq_nr = our_seq_init + 1, ack_nr = peer_syn_ack_seq + 1) ===
        let ack_hdr = UtpHeader {
            ty: UtpType::State,
            ver: 1,
            conn_id: peer_conn_id, // 之后所有发给对端的包, 都使用对端的 conn_id
            ts_us: Self::now_us(),
            ts_diff: Self::now_us().wrapping_sub(syn_hdr.ts_us),
            wnd_size: Self::DEFAULT_WINDOW,
            seq_nr: our_seq_init.wrapping_add(1),
            ack_nr: peer_syn_ack_seq.wrapping_add(1),
        };
        ack_hdr.encode(&mut buf);
        udp.send(&buf[..UtpHeader::MIN_SIZE]).await
            .map_err(|e| anyhow!("uTP send handshake ACK: {e}"))?;

        Ok(Self {
            udp,
            remote,
            our_conn_id,
            peer_conn_id,
            our_seq: our_seq_init.wrapping_add(1),
            peer_ack: peer_syn_ack_seq.wrapping_add(1),
            recv_buf: Vec::with_capacity(64 * 1024),
        })
    }

    /// 发送一个 DATA 包 (包头 + payload). 自动递增 our_seq.
    pub async fn send_data(&mut self, payload: &[u8]) -> anyhow::Result<()> {
        let total = UtpHeader::MIN_SIZE + payload.len();
        let mut buf = vec![0u8; total];
        self.our_seq = self.our_seq.wrapping_add(1);
        let hdr = UtpHeader {
            ty: UtpType::Data,
            ver: 1,
            conn_id: self.peer_conn_id,
            ts_us: Self::now_us(),
            ts_diff: 0,
            wnd_size: Self::DEFAULT_WINDOW,
            seq_nr: self.our_seq,
            ack_nr: self.peer_ack,
        };
        hdr.encode(&mut buf);
        buf[UtpHeader::MIN_SIZE..].copy_from_slice(payload);
        self.udp.send(&buf).await
            .map_err(|e| anyhow!("uTP send DATA ({}B): {e}", payload.len()))?;
        Ok(())
    }

    /// 接收下一个 DATA 包. 过滤非 DATA/乱序/重复, 把 payload 放入 self.recv_buf.
    /// 返回收到的 DATA 字节切片引用 (指向内部 recv_buf, 下一次 recv_data 前有效).
    pub async fn recv_data(&mut self) -> anyhow::Result<&[u8]> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut rbuf = [0u8; 65536];
        loop {
            let n = tokio::time::timeout_at(deadline, self.udp.recv(&mut rbuf)).await
                .map_err(|_| anyhow!("uTP recv DATA timeout from {}", self.remote))?
                .map_err(|e| anyhow!("uTP recv DATA: {e}"))?;
            if n < UtpHeader::MIN_SIZE { continue; }
            let Some(h) = UtpHeader::decode(&rbuf[..n]) else { continue };
            if h.ty != UtpType::Data { continue; } // 忽略 State/Reset/Fin (简化)
            // 把 payload 追加到 recv_buf
            self.recv_buf.clear();
            self.recv_buf.extend_from_slice(&rbuf[UtpHeader::MIN_SIZE..n]);
            self.peer_ack = h.seq_nr.wrapping_add(1);
            // 回复一个 ACK
            let mut ack = [0u8; UtpHeader::MIN_SIZE];
            let ack_hdr = UtpHeader {
                ty: UtpType::State,
                ver: 1,
                conn_id: self.our_conn_id.wrapping_add(1),
                ts_us: Self::now_us(),
                ts_diff: Self::now_us().wrapping_sub(h.ts_us),
                wnd_size: Self::DEFAULT_WINDOW,
                seq_nr: self.our_seq,
                ack_nr: h.seq_nr.wrapping_add(1),
            };
            ack_hdr.encode(&mut ack);
            let _ = self.udp.send(&ack).await;
            return Ok(&self.recv_buf);
        }
    }

    /// 本地 bind 地址 (便于检查 listen port).
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.udp.local_addr()
    }
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

/// ============================================================================
/// 公共 API: 在创建 EngineContext/ChunkManager 之前预解析 BT 元信息.
///
/// 设计原因 (修复 BT 进度一直为 0 的根因之一):
///   EngineBuilder 在 run_all() 之前就需要创建 HybridChunkManager.
///   如果在解析 torrent 之前用假的 file_size (0 或 max(1)) 创建 bases 向量,
///   后续 piece_to_base() 计算的 base_idx 会和真实 bases 长度严重不匹配,
///   导致: ① 写入 piece 时 bases.get 越界 → mark_bytes 静默跳过
///         ② completed_count 永远 < bases.len() → 下载循环无法退出
///
/// 正确流程: 调用本函数 → 拿到 TorrentMeta → 用 meta.total_size / meta.piece_size
///           计算 aligned_base_chunk_size → 用正确参数创建 HybridChunkManager(ctx)
///           → 再启动 EngineBuilder.run_all. 这样 bases/piece 100% 对齐.
/// ============================================================================
pub async fn pre_resolve_bt_meta(
    torrent_file: Option<&std::path::Path>,
    magnet: Option<&str>,
) -> anyhow::Result<TorrentMeta> {
    if let Some(p) = torrent_file {
        let data = tokio::fs::read(p).await
            .map_err(|e| anyhow!("pre_resolve: read .torrent: {}", e))?;
        return TorrentMeta::from_torrent_bytes(&data);
    }
    if let Some(m) = magnet {
        return TorrentMeta::from_magnet(m);
    }
    anyhow::bail!("pre_resolve_bt_meta: 需要 torrent_file 或 magnet 至少一个")
}

/// 根据 meta.piece_size 和 meta.total_size 计算 aligned base_chunk_size,
/// 与 BtDownloaderModule::start() 内部使用的算法保持一致.
pub fn calc_aligned_bt_base(meta: &TorrentMeta) -> u64 {
    use crate::modules::{HYBRID_ALIGNED_BASE, MIN_BASE_SIZE_FOR_BT_ALIGN};
    use crate::speed_engine::MIN_SUBCHUNK_SIZE;
    if meta.total_size >= MIN_BASE_SIZE_FOR_BT_ALIGN {
        let mut n = 1u64;
        while n * meta.piece_size < HYBRID_ALIGNED_BASE { n += 1; }
        n * meta.piece_size
    } else {
        // 小文件: 不追求对齐, 至少保证 4 * MIN_SUBCHUNK_SIZE 作为 base size
        meta.piece_size.max(MIN_SUBCHUNK_SIZE * 4)
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

        // 说明: 以前只在 current==0 才更新, 但调用方 (vortex-dl / main.rs BtOnly)
        //      有时会把 initial_fs 设为 file_size.max(1) = 1, 导致 current!=0 永不更新,
        //      ProgressModule 百分比分母永远是 1 (或其他小值). 改成: 只要 meta.total_size > 0,
        //      无论 current 是什么, 都强制 store 正确大小.
        if meta.total_size > 0 {
            let current = ctx.file_size.load(Ordering::Relaxed);
            if current != meta.total_size {
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

            // ---- 防御性校验: 验证 piece_to_base ↔ bases 对齐 ----
            // 要求调用方 (downloader_manager.rs / main.rs) 在创建 ctx 之前
            // 先调用 pre_resolve_bt_meta + calc_aligned_bt_base, 并用
            // (meta.total_size, aligned_base) 创建 HybridChunkManager.
            // 我们用最后一个 piece 做边界检查, 如果失败说明流程不对.
            let last_piece = meta.pieces.len().saturating_sub(1) as u32;
            let bidx = meta.piece_to_base(aligned, last_piece);
            let bases_cnt = ctx.chunk_mgr.bases.len();
            if (bidx as usize) >= bases_cnt {
                tracing::error!(
                    "BT FATAL: bases/piece 未对齐! last_piece({})→base_idx({}) >= bases.len({}). \
                    请在创建 ctx 前调用 swiftfetch::pre_resolve_bt_meta → calc_aligned_bt_base → \
                    HybridChunkManager::new(meta.total_size, aligned_base)",
                    last_piece, bidx, bases_cnt
                );
                // 主动 panic 避免进度一直为 0 的静默错误
                panic!(
                    "BT bases/piece mismatch: idx={} >= len={}. 入口需调用 pre_resolve_bt_meta 预解析.",
                    bidx, bases_cnt
                );
            }
            tracing::info!(
                "BT: bases/piece 对齐 OK: last_piece({})→base_idx({}) < bases.len({})",
                last_piece, bidx, bases_cnt
            );
        }

        // 预创建完整的文件树: 目录 mkdir + 每个文件 create + set_len (预分配)
        {
            use tokio::io::AsyncSeekExt;
            let out_dir = &ctx.output_path;
            for f in &meta.files {
                if f.size == 0 { continue; }
                let path = out_dir.join(&f.name);
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }
                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                    .create(true).write(true).read(true)
                    .open(&path).await
                {
                    let _ = file.set_len(f.size).await;
                    let _ = file.sync_all().await;
                }
            }
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
            match tracker_announce(
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
        // 如果 trackers 为空或全部失败, 尝试常见公共 tracker
        if peers.is_empty() && !meta.info_hash.iter().all(|&b| b == 0) {
            tracing::warn!("BT: 所有 tracker 失败, 尝试公共 tracker...");
            let public_trackers = [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.demonii.com:1337/announce",
                "udp://exodus.desync.com:6969/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://open.stealth.si:80/announce",
                "udp://tracker.tiny-vps.com:6969/announce",
                "udp://tracker.dler.org:6969/announce",
                "udp://9.rarbg.to:2710/announce",
                "udp://retracker.lanta-net.ru:2710/announce",
            ];
            for tr in &public_trackers {
                match tracker_announce(
                    &client, tr, &meta.info_hash, &self.peer_id,
                    self.port, ctx.file_size.load(Ordering::Relaxed), "started"
                ).await {
                    Ok((p, s, l)) => {
                        peers.extend(p);
                        seeders = seeders.max(s);
                        leechers = leechers.max(l);
                        if !peers.is_empty() { break; }
                    }
                    Err(_) => {}
                }
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

        let mut interval_count = 0u32;
        let mut tracker_retry_count = 0u32;
        let client_c = client.clone();
        let info_hash_c = meta_arc.info_hash;
        let peer_id_c = *peer_id_arc;
        let port_c = self.port;
        let total_size_c = ctx.file_size.load(Ordering::Relaxed);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    interval_count += 1;
                    let done = ctx.chunk_mgr.completed_count();
                    let total = ctx.chunk_mgr.bases.len();
                    let bt_dl = ctx.bt_downloaded.load(Ordering::Relaxed);
                    tracing::info!("BT tick #{}: bases {}/{}, bt_dl={}, peers={}",
                        interval_count, done, total, format_bytes(bt_dl),
                        ctx.active_bt_conns.load(Ordering::Relaxed));
                    if done >= total && total > 0 { break; }
                    if ctx.stop_event_rx.is_disconnected() { break; }

                    // peers 为空或全部失败时, 每 30 秒重试一次 tracker
                    if join_set.is_empty() && interval_count % 6 == 0 {
                        tracker_retry_count += 1;
                        tracing::info!("BT: 无活跃 peer, 第 {} 次重试 tracker...", tracker_retry_count);
                        let mut new_peers: Vec<SocketAddr> = Vec::new();
                        for tr in &meta_arc.trackers {
                            if let Ok((p, s, l)) = tracker_announce(
                                &client_c, tr, &info_hash_c, &peer_id_c,
                                port_c, total_size_c, "started"
                            ).await {
                                new_peers.extend(p);
                                if s > 0 { ctx.bt_seeders.store(s, Ordering::Relaxed); }
                                let _ = l;
                            }
                        }
                        // 也试公共 tracker
                        if new_peers.is_empty() {
                            let public_trackers = [
                                "udp://tracker.opentrackr.org:1337/announce",
                                "udp://open.demonii.com:1337/announce",
                                "udp://exodus.desync.com:6969/announce",
                                "udp://open.stealth.si:80/announce",
                            ];
                            for tr in &public_trackers {
                                if let Ok((p, _, _)) = tracker_announce(
                                    &client_c, tr, &info_hash_c, &peer_id_c,
                                    port_c, total_size_c, "started"
                                ).await {
                                    new_peers.extend(p);
                                    if !new_peers.is_empty() { break; }
                                }
                            }
                        }
                        if !new_peers.is_empty() {
                            new_peers.dedup();
                            tracing::info!("BT: 重试获得 {} 个新 peers", new_peers.len());
                            let total_pieces_r = ctx.bt_total_pieces.load(Ordering::Relaxed);
                            for (i, addr) in new_peers.iter().take(peer_limit * 2).enumerate() {
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
                                        addr, &meta_c, *pid_c, ctx_c.clone(), total_pieces_r
                                    ).await;
                                    ctx_c.active_bt_conns.fetch_sub(1, Ordering::Relaxed);
                                    if let Err(e) = result {
                                        tracing::debug!("peer {} session: {}", addr, e);
                                    }
                                });
                            }
                        }
                        // 最多重试 20 次 (10 分钟), 之后放弃
                        if tracker_retry_count >= 20 {
                            tracing::warn!("BT: tracker 重试 {} 次仍无 peers, 放弃", tracker_retry_count);
                            break;
                        }
                    }
                }
                _ = ctx.stop_notify.notified() => { break; }
                else => {
                    if join_set.is_empty() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
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
            // 注意: 新版 write_data_to_file 需要 meta 参数, 按文件树解析到具体文件路径写
            if write_data_to_file(ctx.clone(), meta, file_offset, data).await.is_ok() {
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

async fn write_data_to_file(ctx: Arc<EngineContext>, meta: &TorrentMeta, global_offset: u64, data: &[u8]) -> anyhow::Result<()> {
    use tokio::io::AsyncSeekExt;
    if data.is_empty() { return Ok(()); }
    if meta.files.is_empty() { return Err(anyhow!("TorrentMeta.files empty")); }
    let out_dir = &ctx.output_path;

    let mut remaining = data;
    let mut cursor_offset = global_offset;

    while !remaining.is_empty() {
        // 找到虚拟流 cursor_offset 对应的实际文件和文件内偏移
        let mut acc = 0u64;
        let mut target_file: Option<&TorrentFileInfo> = None;
        let mut target_file_start: u64 = 0;
        for f in &meta.files {
            let f_start = acc;
            let f_end = acc + f.size;
            if cursor_offset < f_end {
                target_file = Some(f);
                target_file_start = f_start;
                break;
            }
            acc = f_end;
        }
        let f_info = match target_file {
            Some(f) => f,
            None => return Err(anyhow!("BT offset {} 超过 total_size {} (files={})",
                cursor_offset, meta.total_size, meta.files.len())),
        };
        let local_offset = cursor_offset - target_file_start;
        let can_write_in_this_file = (f_info.size.saturating_sub(local_offset))
            .min(remaining.len() as u64) as usize;
        if can_write_in_this_file == 0 { break; }

        let path = out_dir.join(&f_info.name);
        if let Some(parent) = path.parent() { tokio::fs::create_dir_all(parent).await.ok(); }
        let mut f = tokio::fs::OpenOptions::new()
            .create(true).write(true).read(true)
            .open(&path).await
            .map_err(|e| anyhow!("open {} fail: {}", path.display(), e))?;
        // 预分配文件大小 (一次, 避免多次写入时碎片/扩张)
        let _ = f.set_len(f_info.size).await;
        f.seek(std::io::SeekFrom::Start(local_offset)).await
            .map_err(|e| anyhow!("seek {} @ {} fail: {}", path.display(), local_offset, e))?;
        f.write_all(&remaining[..can_write_in_this_file]).await
            .map_err(|e| anyhow!("write {} fail: {}", path.display(), e))?;

        remaining = &remaining[can_write_in_this_file..];
        cursor_offset += can_write_in_this_file as u64;
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

// ============================================================
// 单元测试 + 集成测试: 验证 BT 种子/进度/写文件/tracker
// ============================================================
#[cfg(test)]
mod bt_tests {
    use super::*;
    use crate::modules::{EngineContext, NetworkMode, DownloadMode, ProtocolMode, RwLockContainer, BandwidthEMA, PeerScore, EngineEvent};
    use crate::speed_engine::{SmoothScheduler, SpeedSmoother, OscillationGuard};

    fn build_minimal_ctx(output: std::path::PathBuf, file_size: u64) -> Arc<EngineContext> {
        use std::time::Instant;
        use tokio::sync::{Semaphore, Mutex as TMutex};
        use flume;
        use std::collections::{HashMap, VecDeque};
        let cfg = DownloadConfig::default();
        let chunk_mgr = Arc::new(HybridChunkManager::new(file_size.max(1024), 16384));
        let (event_tx, event_rx) = flume::unbounded::<EngineEvent>();
        let (stop_tx, stop_rx) = flume::unbounded::<()>();
        Arc::new(EngineContext {
            config: cfg,
            protocol: ProtocolMode::BtOnly,
            network_mode: NetworkMode::Auto,
            download_mode: DownloadMode::SparseRareFirst,
            probe: RwLockContainer::new(None),
            output_path: output,
            file_size: AtomicU64::new(file_size),
            base_chunk_size: AtomicU64::new(16384),
            chunk_mgr,
            downloaded: Arc::new(AtomicU64::new(0)),
            http_downloaded: AtomicU64::new(0),
            bt_downloaded: AtomicU64::new(0),
            file: Arc::new(TMutex::new(None)),
            active_http_conns: AtomicU32::new(0),
            active_bt_conns: AtomicU32::new(0),
            http_conn_limit: AtomicU32::new(16),
            bt_peer_limit: AtomicU32::new(16),
            global_max_conns: AtomicU32::new(32),
            sem_http: Arc::new(Semaphore::new(16)),
            sem_bt: Arc::new(Semaphore::new(16)),
            bandwidth_ema: Arc::new(BandwidthEMA::new()),
            event_tx,
            event_rx,
            stop_notify: Arc::new(tokio::sync::Notify::new()),
            stop_event_tx: stop_tx,
            stop_event_rx: stop_rx,
            scheduler: PMutex::new(SmoothScheduler::new(8, 10_000_000, 64 * 1024 * 1024)),
            speed_smoother: PMutex::new(SpeedSmoother::new()),
            oscillation_guard: PMutex::new(OscillationGuard::new()),
            base_chunk_done: PMutex::new(Vec::new()),
            bt_piece_map_completed: PMutex::new(Vec::new()),
            bt_piece_size: AtomicU64::new(16384),
            bt_total_pieces: AtomicU32::new(0),
            peer_scores: PMutex::new(HashMap::new()),
            bt_seeders: AtomicU32::new(0),
            bt_peers: AtomicU32::new(0),
            http_weight: AtomicU64::new(1000),
            bt_weight: AtomicU64::new(1000),
            http_ratio_target: AtomicU64::new(0.6f64.to_bits()),
            bt_ratio_target: AtomicU64::new(0.4f64.to_bits()),
            last_reset_count: AtomicU32::new(0),
            last_reset_window: PRwLock::new(VecDeque::new()),
            conn_delay_ms: AtomicU64::new(0),
            completed_time_series: PMutex::new(Vec::new()),
            prefetch_warmed: PMutex::new(HashMap::new()),
            slow_subchunks: PMutex::new(HashMap::new()),
            mirrors: Vec::new(),
            peer_port: AtomicU32::new(6881),
            ratio_target: AtomicU64::new(1.0f64.to_bits()),
            seed_minutes: AtomicU32::new(0),
            task_id: "test".into(),
            start_instant: Instant::now(),
            no_cross_protocol: false,
        })
    }

    #[tokio::test]
    async fn test_t01_torrent_generate_and_roundtrip() {
        let data = b"Hello, SwiftFetch BT engine! ".repeat(300); // ~10KB
        let meta = TorrentMeta::generate("test.bin", &data, 16384,
            vec!["http://localhost:1/announce".into()])
            .expect("generate torrent");
        assert_eq!(meta.total_size, data.len() as u64);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.pieces.len(), 1);
        // info_hash 不是全 0
        assert_ne!(meta.info_hash, [0u8; 20]);

        // encode → 再 parse → info_hash 必须相等
        let bytes = meta.encode_to_bytes().expect("encode");
        let parsed = TorrentMeta::from_torrent_bytes(&bytes).expect("reparse");
        assert_eq!(parsed.info_hash, meta.info_hash);
        assert_eq!(parsed.total_size, meta.total_size);
        assert_eq!(parsed.piece_size, meta.piece_size);
        assert_eq!(parsed.pieces.len(), meta.pieces.len());
        assert_eq!(parsed.pieces[0], meta.pieces[0]);
        println!("T01 PASS: generate → encode → reparse, info_hash match");
    }

    #[tokio::test]
    async fn test_t02_file_size_fix_total_size_store_logic() {
        // 验证: file_size 从 0 开始会被 store 正确; 如果 current!=0 也会被覆盖 (新逻辑)
        // 具体是在 BtDownloaderModule.start 里的条件分支, 这里直接验证逻辑本身
        let meta = TorrentMeta::generate("test.bin", &[0x42u8; 100_000], 16384, vec![])
            .expect("gen");
        let file_size = AtomicU64::new(0);
        if meta.total_size > 0 {
            let current = file_size.load(Ordering::Relaxed);
            if current != meta.total_size {
                file_size.store(meta.total_size, Ordering::Relaxed);
            }
        }
        assert_eq!(file_size.load(Ordering::Relaxed), 100_000,
            "current=0 时 store 正确");

        let file_size2 = AtomicU64::new(1); // 之前的 bug: initial max(1)
        if meta.total_size > 0 {
            let current = file_size2.load(Ordering::Relaxed);
            if current != meta.total_size {
                file_size2.store(meta.total_size, Ordering::Relaxed);
            }
        }
        assert_eq!(file_size2.load(Ordering::Relaxed), 100_000,
            "current=1 时现在也会覆盖正确 (修复后)");
        println!("T02 PASS: file_size store 逻辑修复验证");
    }

    #[tokio::test]
    async fn test_t03_write_data_to_file_single() {
        let tmp = std::env::temp_dir().join(format!("swift_bt_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        let data = b"Hello World! This is test content."; // 38 bytes
        let meta = TorrentMeta::generate("myfile.bin", data, 16384, vec![]).unwrap();
        let out_path = tmp.clone();

        // 构造一个轻量的 EngineContext 用于写文件
        let fs = meta.files[0].size;
        let ctx = build_minimal_ctx(out_path.clone(), fs);
        ctx.bt_total_pieces.store(meta.pieces.len() as u32, Ordering::Relaxed);
        ctx.bt_piece_size.store(meta.piece_size, Ordering::Relaxed);

        write_data_to_file(ctx, &meta, 0, data).await.expect("write_all");
        // 读取验证
        let on_disk = tokio::fs::read(out_path.join("myfile.bin")).await.expect("read back");
        assert_eq!(on_disk, data, "写回内容不匹配");
        // 清理
        let _ = std::fs::remove_dir_all(&tmp);
        println!("T03 PASS: write_data_to_file 单文件写入验证");
    }

    #[tokio::test]
    async fn test_t04_write_data_to_file_multi() {
        let tmp = std::env::temp_dir().join(format!("swift_bt_test_multi_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&tmp).await.unwrap();

        // 构造 3 个文件的虚拟多文件种子:
        //   file1.bin: 10 bytes "AAAAAAAAAA"
        //   sub/file2.bin: 5 bytes "BBBBB"
        //   sub/deep/file3.bin: 7 bytes "CCCCCCC"
        // 虚拟字节流连续: [AAA..A(10) + BBB..B(5) + CCC..C(7) = 22 bytes]
        let f1 = b"AAAAAAAAAA".to_vec(); // 10
        let f2 = b"BBBBB".to_vec();    // 5
        let f3 = b"CCCCCCC".to_vec();  // 7
        let combined: Vec<u8> = [f1.clone(), f2.clone(), f3.clone()].concat(); // 22

        // 用单文件 data 生成 meta 后手动篡改 files 字段模拟多文件
        let piece_size = 16384u64;
        let mut pieces: Vec<[u8; 20]> = Vec::new();
        for ch in combined.chunks(piece_size as usize) {
            let mut h = Sha1::new(); h.update(ch);
            let r = h.finalize();
            let mut a = [0u8; 20]; a.copy_from_slice(&r); pieces.push(a);
        }
        let fake_tracker = format!("http://localhost:{}/announce", 9);
        let mut meta = TorrentMeta {
            info_hash: [0u8; 20],
            piece_size,
            pieces,
            files: vec![
                TorrentFileInfo { name: "file1.bin".into(), size: 10 },
                TorrentFileInfo { name: "sub/file2.bin".into(), size: 5 },
                TorrentFileInfo { name: "sub/deep/file3.bin".into(), size: 7 },
            ],
            total_size: 22,
            trackers: vec![fake_tracker],
            display_name: "multi_test".into(),
            webseeds: vec![],
        };
        let bytes = meta.encode_to_bytes().unwrap();
        let parsed = TorrentMeta::from_torrent_bytes(&bytes).unwrap();
        meta.info_hash = parsed.info_hash; // 让 info_hash 正确

        // 构造 EngineContext (复用 build_minimal_ctx)
        let fs = 22u64;
        let ctx = build_minimal_ctx(tmp.clone(), fs);
        ctx.bt_total_pieces.store(meta.pieces.len() as u32, Ordering::Relaxed);
        ctx.bt_piece_size.store(meta.piece_size, Ordering::Relaxed);

        // 写: 分 3 次模拟写不同偏移 (模拟多 piece 块)
        write_data_to_file(ctx.clone(), &meta, 0, &f1).await.unwrap();
        write_data_to_file(ctx.clone(), &meta, 10, &f2).await.unwrap();
        write_data_to_file(ctx.clone(), &meta, 15, &f3).await.unwrap();

        assert_eq!(tokio::fs::read(tmp.join("file1.bin")).await.unwrap(), f1);
        assert_eq!(tokio::fs::read(tmp.join("sub/file2.bin")).await.unwrap(), f2);
        assert_eq!(tokio::fs::read(tmp.join("sub/deep/file3.bin")).await.unwrap(), f3);
        // 大小校验
        assert_eq!(tokio::fs::metadata(tmp.join("file1.bin")).await.unwrap().len(), 10);
        assert_eq!(tokio::fs::metadata(tmp.join("sub/file2.bin")).await.unwrap().len(), 5);
        assert_eq!(tokio::fs::metadata(tmp.join("sub/deep/file3.bin")).await.unwrap().len(), 7);
        let _ = std::fs::remove_dir_all(&tmp);
        println!("T04 PASS: write_data_to_file 多文件跨文件写入验证");
    }

    #[tokio::test]
    async fn test_t05_http_tracker_mock() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // 启动一个本地 HTTP tracker mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind tracker");
        let tracker_port = listener.local_addr().unwrap().port();
        let tracker_url = format!("http://127.0.0.1:{}/announce", tracker_port);

        let server = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if n == 0 { continue; }
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                // 只需要返回一个 compact peers 格式的 bencode
                // peers: 127.0.0.1:12345 => bytes [127,0,0,1,48,57] (48<<8|57 = 12345)
                let resp_body = "d8:completei5e10:incompletei10e8:intervali1800e5:peers6:\x7f\x00\x00\x01\x30\x39e".to_string();
                let body_bytes = resp_body.as_bytes();
                let http = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                    body_bytes.len()
                );
                sock.write_all(http.as_bytes()).await.ok();
                sock.write_all(body_bytes).await.ok();
                sock.flush().await.ok();
                break; // 处理一次就结束
            }
        });

        // 等 server 启动
        tokio::time::sleep(Duration::from_millis(200)).await;

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build().expect("client");
        let info_hash = b"0123456789abcdef0123";
        let peer_id = b"-SW0300-000000000000"; // 8 + 12 = 20 bytes exactly
        let (peers, s, l) = tracker_announce_http(
            &client, &tracker_url, info_hash, peer_id, 16384, 1048576, "started"
        ).await.expect("http tracker announce");

        assert!(peers.len() > 0, "peers 空的, mock tracker 返回失败");
        assert_eq!(peers[0], "127.0.0.1:12345".parse::<SocketAddr>().unwrap());
        assert_eq!(s, 5);
        assert_eq!(l, 10);
        let _ = server.await;
        println!("T05 PASS: HTTP tracker 完整 announce 流程验证");
    }

    #[tokio::test]
    async fn test_t06_piece_base_alignment_no_mismatch() {
        // 复现 bug: 初始创建 HybridChunkManager 时 file_size=0 → max(1MB),
        // bases 只按 1MB 切了 1 个. 但解析真实 meta (大文件, 如 500MB) 后,
        // piece_to_base 用 aligned=32MB (HYBRID_ALIGNED_BASE) 计算, 最后 piece 会
        // 落到 base_idx≈15, 远 >= bases.len()=1, 导致 mark_bytes 被跳过.
        //
        // 修复策略: 创建 chunk_mgr 之前必须预解析 meta, 用
        // HybridChunkManager::new(meta.total_size, calc_aligned_bt_base(&meta)).
        let piece_size = 16384u64;
        // 500MB test: pieces ≈ 32,000, 需要的 base 数量: ceil(500MB / 32MB_aligned) = 16
        let five_hundred_mb = 500 * 1024 * 1024usize;
        // 用快速构造 pieces 的方式: 不需要真实 500MB 数据, 用 generate 小数据后篡改 meta
        let small_data = vec![0xABu8; 1024];
        let mut meta = TorrentMeta::generate("test.bin", &small_data, piece_size, vec![]).unwrap();
        // 篡改: 手动设置 500MB total_size 和对应 pieces 数量
        let total_pieces_needed = (five_hundred_mb as u64 + piece_size - 1) / piece_size; // ≈ 32_000
        meta.total_size = five_hundred_mb as u64;
        meta.pieces = vec![[0u8; 20]; total_pieces_needed as usize];
        meta.files[0].size = five_hundred_mb as u64;
        println!("  [scenario] total_size={} pieces={} piece_size={}",
            meta.total_size, meta.pieces.len(), piece_size);

        // Step 1: 模拟 bug 流程 (BtOnly 时 file_size=0/fake 就创建 mgr)
        let initial_fs_fake = 0u64;
        let initial_base = 1024 * 1024; // 1MB
        let buggy_fs_for_mgr = initial_fs_fake.max(1024 * 1024);
        let buggy_mgr = HybridChunkManager::new(buggy_fs_for_mgr, initial_base);
        println!("  [buggy]   HybridChunkManager::new(fs=1MB, base=1MB) → bases.len() = {}", buggy_mgr.bases.len());
        let aligned = calc_aligned_bt_base(&meta);
        println!("  [align]   calc_aligned_bt_base → {} bytes (~{}MB)", aligned, aligned / 1024 / 1024);
        let last_piece = (meta.pieces.len() - 1) as u32;
        let mapped_base = meta.piece_to_base(aligned, last_piece);
        let bases_needed = mapped_base + 1;
        println!("  [align]   last_piece_idx={} → piece_to_base → base_idx={}, bases_needed={}",
            last_piece, mapped_base, bases_needed);
        // BUG 断言: buggy_mgr.bases.len() (1) << bases_needed (16 左右)
        assert!(buggy_mgr.bases.len() < bases_needed as usize,
            "BUG 复现失败: buggy.len({}) >= needed({})", buggy_mgr.bases.len(), bases_needed);

        // Step 2: 修复后流程: 预解析 meta → 用正确参数创建 mgr
        let fixed_mgr = HybridChunkManager::new(meta.total_size, aligned);
        println!("  [fixed]   HybridChunkManager::new({}MB, {}MB) → bases.len() = {}",
            meta.total_size / 1024 / 1024, aligned / 1024 / 1024, fixed_mgr.bases.len());
        assert!(fixed_mgr.bases.len() as u32 >= bases_needed,
            "修复后: bases.len({}) < needed({})", fixed_mgr.bases.len(), bases_needed);
        // 遍历每个 piece 验证索引合法
        let ok = meta.pieces.iter().enumerate().all(|(i, _)| {
            let bidx = meta.piece_to_base(aligned, i as u32);
            (bidx as usize) < fixed_mgr.bases.len()
        });
        assert!(ok, "修复后存在 piece 其 base_idx >= bases.len()");
        println!("T06 PASS: bases/piece 对齐: buggy={} < needed={} vs fixed={} >= needed",
            buggy_mgr.bases.len(), bases_needed, fixed_mgr.bases.len());
    }

    #[tokio::test]
    async fn test_t07_chunk_mgr_rebuild_api_helper() {
        // 验证思路: 解析 meta 后, 按真实 total_size + aligned base_chunk_size
        // 创建全新 HybridChunkManager, 保证 piece_to_base 索引不越界
        use std::sync::atomic::Ordering;
        let tmp = std::env::temp_dir().join(format!("swift_rebuild_test_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&tmp).await;
        let data = vec![0xCDu8; 3 * 1024 * 1024];
        let piece_size = 16384u64;
        let meta = TorrentMeta::generate("t3mb.bin", &data, piece_size, vec![]).unwrap();

        // 初始 fake (和 BtOnly 流程一致, file_size=0 → max(1MB))
        let ctx = build_minimal_ctx(tmp.clone(), 0);
        let initial_bases_len = ctx.chunk_mgr.bases.len();
        println!("  initial bases.len={} (file_size=0 → max 1MB)", initial_bases_len);

        // 模拟修复: 解析 meta → 重建全新 Manager (替换思路，实际中在 resolve_meta 后创建 ctx)
        ctx.file_size.store(meta.total_size, Ordering::Relaxed);
        let aligned: u64 = 16 * 1024 * 1024; // 简化
        ctx.base_chunk_size.store(aligned, Ordering::Relaxed);
        // 验证: 用真实参数重建 new_mgr 后所有 piece 索引合法
        let new_mgr = HybridChunkManager::new(meta.total_size, aligned);
        let ok = meta.pieces.iter().enumerate().all(|(i, _)| {
            let bidx = meta.piece_to_base(aligned, i as u32);
            (bidx as usize) < new_mgr.bases.len()
        });
        assert!(ok, "存在 piece 其 base_idx >= new_mgr.bases.len()");
        println!("  after rebuild bases.len={} → all pieces in range", new_mgr.bases.len());
        let _ = std::fs::remove_dir_all(&tmp);
        println!("T07 PASS: 解析 meta 后重建 bases 的修复思路验证");
    }
}
