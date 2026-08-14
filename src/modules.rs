//! SwiftFetch v3 多模块并行启动架构
//!
//! 核心抽象：DownloadModule trait + EngineContext 全局状态容器
//! 所有模块通过 tokio::spawn + JoinSet 并行启动

use async_trait::async_trait;
use flume::{Sender, Receiver};
use parking_lot::{Mutex as PMutex, RwLock as PRwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::sync::{Notify, Semaphore, Mutex as TMutex};
use tokio::task::JoinSet;

use crate::speed_engine::{
    BaseChunk, HybridChunkManager, SmoothScheduler, SpeedSmoother, OscillationGuard,
    ProbeResult, SubChunk, DownloadConfig, ProgressInfo, DownloadResult,
    MIN_SUBCHUNK_SIZE, format_bytes, format_speed, format_progress_bar,
};

// ============================================================
// 全局常量
// ============================================================

pub const MIN_BASE_SIZE_FOR_BT_ALIGN: u64 = 1024 * 1024 * 1024;
pub const HYBRID_ALIGNED_BASE: u64 = 32 * 1024 * 1024;
pub const PREFETCH_WARM_BYTES: usize = 16 * 1024;
pub const BT_REQUEST_BLOCK: u64 = 16 * 1024;
pub const DEFAULT_PEER_LIMIT: u32 = 64;
pub const FIVEG_PEER_LIMIT: u32 = 24;
pub const DEFAULT_GLOBAL_MAX_CONNS: u32 = 96;
pub const FIVEG_GLOBAL_MAX_CONNS: u32 = 48;
pub const FIVEG_HTTP_MAX_CONNS: u32 = 18;
pub const DEFAULT_BT_PORT_START: u16 = 6881;
pub const DEFAULT_BT_PORT_END: u16 = 6889;
pub const DEFAULT_RATIO: f64 = 1.0;
pub const DEFAULT_SEED_MINUTES: u32 = 0;

// ============================================================
// 网络模式
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkMode {
    Auto,
    FiveG,
    Wired1G,
    Wired25G,
}

impl Default for NetworkMode {
    fn default() -> Self { NetworkMode::Auto }
}

// ============================================================
// 下载模式
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadMode {
    SparseRareFirst,
    SequentialStream,
}

impl Default for DownloadMode {
    fn default() -> Self { DownloadMode::SparseRareFirst }
}

// ============================================================
// 协议模式
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolMode {
    Hybrid,
    HttpOnly,
    BtOnly,
}

impl Default for ProtocolMode {
    fn default() -> Self { ProtocolMode::Hybrid }
}

// ============================================================
// 子分片源协议提示 (Http / Bitorrent / Any)
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHint {
    Http,
    Bitorrent,
    Any,
}

impl Default for SourceHint {
    fn default() -> Self { SourceHint::Any }
}

// ============================================================
// 引擎事件 (flume 通道)
// ============================================================

#[derive(Debug, Clone)]
pub enum EngineEvent {
    Stop,
    FatalError(String),
    HttpBoost(i32),
    BtBoost(i32),
    BandwidthRatio { http: f64, bt: f64 },
    SysInfo(String),
    HotResource { protocol: ProtocolMode, weight: f64 },
    ColdResource { protocol: ProtocolMode, weight: f64 },
    NatOverload,
}

// ============================================================
// HTTP EMA / BT EMA 共享
// ============================================================

#[derive(Debug)]
pub struct BandwidthEMA {
    pub http_ema: AtomicU64,
    pub bt_ema: AtomicU64,
    last_http_bytes: AtomicU64,
    last_bt_bytes: AtomicU64,
    last_tick: PRwLock<Instant>,
}

impl BandwidthEMA {
    pub fn new() -> Self {
        Self {
            http_ema: AtomicU64::new(0),
            bt_ema: AtomicU64::new(0),
            last_http_bytes: AtomicU64::new(0),
            last_bt_bytes: AtomicU64::new(0),
            last_tick: PRwLock::new(Instant::now()),
        }
    }

    pub fn tick(&self, http_bytes: u64, bt_bytes: u64, alpha: f64) -> (u64, u64) {
        let now = Instant::now();
        let mut lt = self.last_tick.write();
        let dt = now.duration_since(*lt).as_secs_f64().max(0.001);
        *lt = now;
        drop(lt);

        let last_http = self.last_http_bytes.swap(http_bytes, Ordering::Relaxed);
        let last_bt = self.last_bt_bytes.swap(bt_bytes, Ordering::Relaxed);
        let http_delta = http_bytes.saturating_sub(last_http);
        let bt_delta = bt_bytes.saturating_sub(last_bt);
        let http_inst = (http_delta as f64 / dt) as u64;
        let bt_inst = (bt_delta as f64 / dt) as u64;

        let prev_http = self.http_ema.load(Ordering::Relaxed) as f64;
        let prev_bt = self.bt_ema.load(Ordering::Relaxed) as f64;
        let new_http = (alpha * prev_http + (1.0 - alpha) * http_inst as f64) as u64;
        let new_bt = (alpha * prev_bt + (1.0 - alpha) * bt_inst as f64) as u64;
        self.http_ema.store(new_http, Ordering::Relaxed);
        self.bt_ema.store(new_bt, Ordering::Relaxed);
        (new_http, new_bt)
    }

    pub fn global_ema(&self) -> u64 {
        self.http_ema.load(Ordering::Relaxed).saturating_add(
            self.bt_ema.load(Ordering::Relaxed)
        )
    }
}

// ============================================================
// Peer 评分
// ============================================================

#[derive(Debug, Clone)]
pub struct PeerScore {
    pub addr: String,
    pub ema_speed: f64,
    pub pieces_sent: u32,
    pub errors: u32,
    pub rtt_ms: u64,
    pub timeouts: u32,
    pub banned_until: Option<Instant>,
}

impl PeerScore {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            ema_speed: 0.0,
            pieces_sent: 0,
            errors: 0,
            rtt_ms: 0,
            timeouts: 0,
            banned_until: None,
        }
    }

    pub fn is_banned(&self) -> bool {
        matches!(self.banned_until, Some(t) if t > Instant::now())
    }

    pub fn update_speed(&mut self, bytes: u64, dt_secs: f64) {
        if dt_secs <= 0.0 { return; }
        let inst = bytes as f64 / dt_secs;
        let alpha = 0.8;
        self.ema_speed = alpha * self.ema_speed + (1.0 - alpha) * inst;
    }
}

// ============================================================
// EngineContext - 全局状态容器
// ============================================================

pub struct EngineContext {
    pub config: DownloadConfig,
    pub protocol: ProtocolMode,
    pub network_mode: NetworkMode,
    pub download_mode: DownloadMode,
    pub probe: RwLockContainer<Option<ProbeResult>>,
    pub output_path: PathBuf,
    pub file_size: AtomicU64,
    pub base_chunk_size: AtomicU64,
    pub chunk_mgr: Arc<HybridChunkManager>,
    pub downloaded: Arc<AtomicU64>,
    pub http_downloaded: AtomicU64,
    pub bt_downloaded: AtomicU64,
    pub file: Arc<TMutex<Option<File>>>,
    pub active_http_conns: AtomicU32,
    pub active_bt_conns: AtomicU32,
    pub http_conn_limit: AtomicU32,
    pub bt_peer_limit: AtomicU32,
    pub global_max_conns: AtomicU32,
    pub sem_http: Arc<Semaphore>,
    pub sem_bt: Arc<Semaphore>,
    pub bandwidth_ema: Arc<BandwidthEMA>,
    pub event_tx: Sender<EngineEvent>,
    pub event_rx: Receiver<EngineEvent>,
    pub stop_notify: Arc<Notify>,
    pub stop_event_tx: Sender<()>,
    pub stop_event_rx: Receiver<()>,
    pub scheduler: PMutex<SmoothScheduler>,
    pub speed_smoother: PMutex<SpeedSmoother>,
    pub oscillation_guard: PMutex<OscillationGuard>,
    pub base_chunk_done: PMutex<Vec<u32>>,
    pub bt_piece_map_completed: PMutex<Vec<u32>>,
    pub bt_piece_size: AtomicU64,
    pub bt_total_pieces: AtomicU32,
    pub peer_scores: PMutex<HashMap<String, PeerScore>>,
    pub bt_seeders: AtomicU32,
    pub bt_peers: AtomicU32,
    pub http_weight: std::sync::atomic::AtomicU64,
    pub bt_weight: std::sync::atomic::AtomicU64,
    pub http_ratio_target: std::sync::atomic::AtomicU64,
    pub bt_ratio_target: std::sync::atomic::AtomicU64,
    pub last_reset_count: AtomicU32,
    pub last_reset_window: PRwLock<VecDeque<(Instant, bool)>>,
    pub conn_delay_ms: AtomicU64,
    pub completed_time_series: PMutex<Vec<(u32, Instant)>>,
    pub prefetch_warmed: PMutex<HashMap<u32, bytes::Bytes>>,
    pub slow_subchunks: PMutex<HashMap<u64, (Instant, u64, Option<tokio::task::JoinHandle<()>>)>>,
    pub mirrors: Vec<String>,
    pub peer_port: AtomicU32,
    pub ratio_target: std::sync::atomic::AtomicU64,
    pub seed_minutes: AtomicU32,
    pub task_id: String,
    pub start_instant: Instant,
    pub no_cross_protocol: bool,
}

pub struct RwLockContainer<T> {
    inner: PRwLock<T>,
}

impl<T> RwLockContainer<T> {
    pub fn new(v: T) -> Self { Self { inner: PRwLock::new(v) } }
    pub fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R { f(&self.inner.read()) }
    pub fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R { f(&mut self.inner.write()) }
}

// ============================================================
// DownloadModule trait
// ============================================================

#[async_trait]
pub trait DownloadModule: Send + Sync {
    fn name(&self) -> &'static str;
    async fn start(self: Arc<Self>, ctx: Arc<EngineContext>) -> anyhow::Result<()>;
}

// ============================================================
// EngineBuilder
// ============================================================

pub struct EngineBuilder {
    modules: Vec<Arc<dyn DownloadModule>>,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self { modules: Vec::new() }
    }

    pub fn register<M: DownloadModule + 'static>(mut self, module: M) -> Self {
        self.modules.push(Arc::new(module));
        self
    }

    pub fn register_arc(mut self, module: Arc<dyn DownloadModule>) -> Self {
        self.modules.push(module);
        self
    }

    pub fn modules(&self) -> &[Arc<dyn DownloadModule>] {
        &self.modules
    }

    pub async fn run_all(self, ctx: Arc<EngineContext>) -> anyhow::Result<()> {
        let mut set: JoinSet<anyhow::Result<()>> = JoinSet::new();
        for module in self.modules {
            let ctx_c = ctx.clone();
            let name = module.name().to_string();
            set.spawn(async move {
                tracing::info!("模块启动: {}", name);
                let res = module.start(ctx_c).await;
                if let Err(ref e) = res {
                    tracing::error!("模块 {} 致命错误: {:#}", name, e);
                } else {
                    tracing::info!("模块完成: {}", name);
                }
                res
            });
        }

        let mut fatal: Option<String> = None;
        let mut all_done = false;
        while !all_done {
            tokio::select! {
                Some(res) = set.join_next() => {
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            if fatal.is_none() {
                                fatal = Some(e.to_string());
                                let _ = ctx.event_tx.send(EngineEvent::FatalError(e.to_string()));
                                ctx.stop_notify.notify_waiters();
                                let _ = ctx.stop_event_tx.send(());
                            }
                        }
                        Err(join_err) => {
                            if fatal.is_none() {
                                fatal = Some(format!("模块任务panic: {}", join_err));
                                let _ = ctx.event_tx.send(EngineEvent::FatalError(fatal.clone().unwrap()));
                                ctx.stop_notify.notify_waiters();
                                let _ = ctx.stop_event_tx.send(());
                            }
                        }
                    }
                }
                _ = ctx.stop_event_rx.recv_async() => {
                    all_done = true;
                }
                else => {
                    if set.is_empty() {
                        all_done = true;
                    }
                }
            }
        }

        set.shutdown().await;
        if let Some(msg) = fatal {
            anyhow::bail!(msg);
        }
        Ok(())
    }
}

impl Default for EngineBuilder {
    fn default() -> Self { Self::new() }
}

// ============================================================
// SubChunk 扩展 (source_hint)
// ============================================================

#[derive(Debug, Clone)]
pub struct HybridSubChunk {
    pub inner: SubChunk,
    pub source_hint: SourceHint,
}

// ============================================================
// 辅助: 打包 f64 到 AtomicU64
// ============================================================

pub fn f64_to_atomic_store(atom: &std::sync::atomic::AtomicU64, v: f64) {
    atom.store(v.to_bits(), Ordering::Relaxed);
}

pub fn f64_from_atomic_load(atom: &std::sync::atomic::AtomicU64) -> f64 {
    f64::from_bits(atom.load(Ordering::Relaxed))
}
