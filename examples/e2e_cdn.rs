//! 端到端验证: 用**真实的** dynamic_engine 下载一个 CDN 文件, 统计 429 次数与吞吐。
//!
//! 用途 (2026-10-02): 验证"请求令牌桶 + 采纳 Retry-After"两项修复是否真的
//! 让下载不再撞 429 限流。旧行为: 开局 192 条流齐发 → 55 个 429 → 重试风暴 →
//! 尾部速度掉到 B/s。
//!
//! 用法:
//!   cargo run --release --example e2e_cdn -- <url> [输出文件]
//!
//! 引擎自身的日志在同目录的 logs/download_engine.log。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use swiftfetch::dynamic_engine::{
    download_file, runtime_worker_threads, streams_for_threads, DownloadEngine,
    EngineState as DynEngineState,
};
use swiftfetch::speed_engine::{DownloadConfig, ProgressInfo};
use tokio::sync::{watch, Notify};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args.next().expect("用法: e2e_cdn <url> [输出文件]");
    let out = args.next().unwrap_or_else(|| "e2e_out.bin".to_string());

    let threads = runtime_worker_threads();
    // ★ 授权限速比例: 与 VortexDL/src-tauri/src/downloader.rs 的做法一致 ——
    //   免费版 0.8 用**占空比**限速 (每秒只给 80% 的时间收数据), 不缩放连接数。
    let ratio: f64 = std::env::var("SF_SPEED_RATIO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1.0);
    let streams = streams_for_threads(threads);

    let mut cfg = DownloadConfig::default();
    cfg.url = url.clone();
    cfg.output = Some(out.clone().into());
    cfg.connections = streams;
    // 每次都是干净下载, 排除断点续传的干扰
    cfg.resume_enabled = false;
    cfg.auto_adjust = true;

    let (state_tx, state_rx) = watch::channel(DynEngineState::Starting);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let resume_notify = Arc::new(Notify::new());

    // ★ 可选: 只下 N 字节就停 (SF_MAX_BYTES)。
    //   用途: 在**高速不限流**的源上做限速对比 —— 不必下完整个大文件,
    //   而且没有 429 静默干扰, 比例才算得准。
    let max_bytes: Option<u64> = std::env::var("SF_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok());

    let engine = Arc::new(
        DownloadEngine::new(
            cfg,
            state_tx,
            state_rx,
            cancel_flag.clone(),
            resume_notify,
        )
        .await?
        .with_task_id("e2e".into())
        .with_speed_ratio(ratio),
    );

    if let Some(limit) = max_bytes {
        let e = engine.clone();
        let flag = cancel_flag.clone();
        tokio::spawn(async move {
            loop {
                if e.pool.total_downloaded() >= limit {
                    flag.store(true, Ordering::Relaxed);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    println!(
        "[e2e] 线程={} 授权比例={} 并发流={} 文件大小={} 种子块={} worker={}",
        threads,
        ratio,
        streams,
        engine.pool.file_size,
        engine.pool.chunks_count(),
        engine.worker_count
    );

    let start = Instant::now();
    let last_log = Arc::new(Mutex::new(Instant::now()));
    let cb = {
        let last_log = last_log.clone();
        Arc::new(move |info: ProgressInfo| {
            let mut g = last_log.lock().unwrap();
            if g.elapsed() >= Duration::from_secs(3) {
                *g = Instant::now();
                println!(
                    "[e2e] {:5.1}%  {:6.2} MB/s  active={}  eta={}s  state={}",
                    info.progress,
                    info.speed_bps as f64 / 1_048_576.0,
                    info.active_conns,
                    info.eta_sec.unwrap_or(0),
                    info.state
                );
            }
        })
    };

    let res = download_file(engine.clone(), cb).await;
    let elapsed = start.elapsed().as_secs_f64();
    let dl = engine.pool.total_downloaded();

    // 设了字节上限时, 取消是预期行为, 不算失败
    let tag = match &res {
        Ok(_) => "成功".to_string(),
        Err(_) if max_bytes.is_some() => format!("到达上限 {} 字节 (预期)", max_bytes.unwrap()),
        Err(e) => format!("失败: {}", e),
    };

    println!("----------------------------------------------");
    println!(
        "[e2e] 结果={} 下载={} 字节 耗时={:.1}s 平均={:.2} MB/s",
        tag,
        dl,
        elapsed,
        if elapsed > 0.0 { dl as f64 / 1_048_576.0 / elapsed } else { 0.0 }
    );
    println!(
        "[e2e] 429 次数={}  令牌桶限速次数={}",
        engine.total_429.load(Ordering::Relaxed),
        engine.req_bucket.blocked_count()
    );
    // ★ 限速器读数: 这是"免费版是否真 80%"最直接的证据 ——
    //   上限应当 ≈ 测得的原速 × ratio。
    {
        let t = &engine.throttle;
        let mb = 1_048_576.0;
        println!(
            "[e2e] 限速器: 预热测得原速 {:.2} MB/s → 上限 {:.2} MB/s (设定比例 {:.0}%, 实际 {:.1}%)",
            t.baseline_bps() as f64 / mb,
            t.cap_bps() as f64 / mb,
            ratio * 100.0,
            if t.baseline_bps() > 0 {
                t.cap_bps() as f64 / t.baseline_bps() as f64 * 100.0
            } else {
                0.0
            }
        );
    }

    if let Err(e) = res {
        return Err(format!("下载失败: {}", e).into());
    }
    Ok(())
}
