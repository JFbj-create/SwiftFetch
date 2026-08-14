//! SwiftFetch v3 - 纯CLI高性能无UI下载内核
//!
//! v3 插件化解耦架构:
//! - 插件层: src/plugin.rs (AsyncThreadPlugin / IsolatedProcessPlugin + PluginRegistry)
//! - IPC 层:  src/ipc.rs    (JSON Lines + 消息节流 + 命名管道)
//! - 调度层:  src/host.rs   (PluginHost + ResumeWriterActor + 内置薄包装插件)
//! - 业务层:  speed_engine.rs / bt_engine.rs / modules.rs (原有逻辑, 不改业务)
//! - 协议抽象: src/protocols.rs (统一 ProtocolProvider trait + capability bitflags)
//! - 协议实现: src/protocols_impls.rs (HTTP1/2/3 FTP(S) SFTP WebDAV rsync IPFS)
//!
//! HTTP 特性: 多源镜像聚合, 慢分片重调度, 分片预取, TCP/HTTP2 自动调优
//! BT 特性  : 自研 wire protocol, magnet/.torrent, HTTP/BT 基底块 32MB 对齐
//! 新协议特性: FTP/FTPS (suppaftp), SFTP (openssh-sftp-client / ssh2), WebDAV (reqwest_dav),
//!             rsync (libsync3 xxhash3 + SSH), IPFS (Kubo RPC / Gateway), HTTP/3 (reqwest+quinn)

pub mod modules;
pub mod speed_engine;
pub mod bt_engine;
pub mod plugin;
pub mod ipc;
pub mod host;
pub mod protocols;
pub mod protocols_impls;

pub use modules::{
    DownloadModule, EngineBuilder, EngineContext, EngineEvent, PeerScore,
    BandwidthEMA, NetworkMode, DownloadMode, ProtocolMode, SourceHint,
    HybridSubChunk, RwLockContainer,
    MIN_BASE_SIZE_FOR_BT_ALIGN, HYBRID_ALIGNED_BASE, PREFETCH_WARM_BYTES,
    BT_REQUEST_BLOCK, DEFAULT_PEER_LIMIT, FIVEG_PEER_LIMIT,
    DEFAULT_GLOBAL_MAX_CONNS, FIVEG_GLOBAL_MAX_CONNS, FIVEG_HTTP_MAX_CONNS,
    DEFAULT_BT_PORT_START, DEFAULT_BT_PORT_END, DEFAULT_RATIO, DEFAULT_SEED_MINUTES,
    f64_to_atomic_store, f64_from_atomic_load,
};

pub use speed_engine::{
    SwiftFetch,
    DownloadConfig,
    DownloadResult,
    ProgressInfo,
    ProbeResult,
    HybridChunkManager,
    BaseChunk,
    SubChunk,
    SmoothScheduler,
    SpeedSmoother,
    OscillationGuard,
    AcquiredWork,
    SchedulerDecision,
    OscillationState,
    ResumeFile,
    HttpDownloaderModule,
    ProbeModule,
    PrefetchModule,
    OscillationGuardModule,
    SchedulerModule,
    BandwidthPoolModule,
    NATSessionGuardModule,
    ProgressModule,
    build_reqwest_client,
    MAX_CONNECTIONS_PER_HOST,
    DEFAULT_CONNECTIONS,
    TIMEOUT_CONNECT,
    TIMEOUT_READ,
    TIMEOUT_REQUEST,
    SUBCHUNK_READ_TIMEOUT,
    MIN_SUBCHUNK_SIZE,
    WORK_STEAL_REMAIN,
    PROBE_SAMPLE_BYTES,
    SPEED_SAMPLE_MS,
    SCHEDULER_COOLDOWN_MS,
    OSCILLATION_WINDOW_MS,
    OSCILLATION_THRESHOLD,
    OSCILLATION_UNFREEZE,
    FREEZE_DURATION_MS,
    EMA_ALPHA,
    SLOW_CHUNK_FACTOR,
    MAX_REDIRECTS,
    MAX_RETRIES,
    RESUME_EXT,
    format_speed,
    format_bytes,
    format_progress_bar,
};

pub use bt_engine::{
    BtDownloaderModule,
    TorrentMeta,
    TorrentFileInfo,
    BenValue,
    BenParser,
    BtMessage,
    PeerConnState,
    generate_peer_id,
    tracker_announce_http,
    peer_connect,
};

pub use plugin::{
    PluginId, PluginKind, PluginHealth, SwiftPlugin, PluginRegistry,
    PluginResult, PluginBox, PluginHost, HostBusMsg, PluginEventMsg,
    PluginMsg, PluginReply, ConnectionPool, ConnStats, ResumeDeltaMsg,
    AsyncThreadPlugin, IsolatedProcessPlugin, generate_req_id, IpcFrame,
};

pub use ipc::{
    HttpMethod, BtMethod, SchedMethod, ResumeMethod, EventTopic,
    MessageThrottler, CrashBackoff, IpcFramedReader, IpcFramedWriter,
    make_ipc_reader, make_ipc_writer, validate_req_id,
    make_request, make_reply, make_event,
};

pub use host::{
    HttpDownloaderPlugin, BtDownloaderPlugin, ProbePrefetchPlugin,
    SchedulerPlugin, PluginHostRuntime, ResumeWriterActor, format_plugin_table,
};

// ===== protocols (协议抽象 + 实现) =====
pub use protocols::{
    ProtocolProvider, ProtocolCapability, UrlScheme, AuthInfo,
    ResourceMeta, DirEntry, RangeRequest, ByteStream, ProviderRegistry, ProviderBox,
};
pub use protocols_impls::register_all_feature_providers;
pub use protocols_impls::{simple_provider_download, needs_provider_dispatch};

#[cfg(any(feature = "http", feature = "http2", feature = "http3"))]
pub use protocols_impls::HttpFamilyProvider;
#[cfg(feature = "ftp")]
pub use protocols_impls::FtpProvider;
#[cfg(feature = "webdav")]
pub use protocols_impls::WebdavProvider;
#[cfg(feature = "sftp")]
pub use protocols_impls::SftpProvider;
#[cfg(feature = "rsync")]
pub use protocols_impls::RsyncProvider;
#[cfg(feature = "ipfs")]
pub use protocols_impls::IpfsProvider;
