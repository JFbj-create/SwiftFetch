// 动态引擎测速工具: 直接驱动 VortexDL 应用所用的 dynamic_engine
// 用法: dyn_speed <url> <out> [conns] [secs]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};

use swiftfetch::{download_file, DownloadConfig, DownloadEngine, DynEngineState, ProgressInfo};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url = args.get(1).cloned().unwrap_or_default();
    let out = args.get(2).cloned().unwrap_or_else(|| "dyn_test.bin".into());
    let conns: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    if url.is_empty() {
        eprintln!("usage: dyn_speed <url> <out> [conns] [secs]");
        std::process::exit(2);
    }
    if let Some(p) = std::path::Path::new(&out).parent() {
        let _ = std::fs::create_dir_all(p);
    }

    let mut cfg = DownloadConfig::default();
    cfg.url = url.clone();
    cfg.output = Some(out.clone().into());
    cfg.connections = conns;
    cfg.resume_enabled = false;
    cfg.headers = DownloadConfig::default_headers();

    let (state_tx, state_rx) = watch::channel(DynEngineState::Starting);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let resume_notify = Arc::new(Notify::new());

    let engine = match DownloadEngine::new(
        cfg,
        state_tx,
        state_rx,
        cancel_flag.clone(),
        resume_notify,
    )
    .await
    {
        Ok(e) => Arc::new(e.with_task_id("dyn_speed".into())),
        Err(e) => {
            eprintln!("engine init failed: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!(
        "[init] file_size={} chunks={} workers={}",
        engine.pool.file_size,
        engine.pool.chunks_count(),
        engine.worker_count
    );

    let t0 = Instant::now();
    let last = Arc::new(Mutex::new((0u64, Instant::now())));
    let cb_last = last.clone();
    let cb: Arc<dyn Fn(ProgressInfo) + Send + Sync> = Arc::new(move |info: ProgressInfo| {
        let mut l = cb_last.lock().unwrap();
        let dt = l.1.elapsed().as_secs_f64();
        if dt >= 1.0 {
            let db = info.downloaded.saturating_sub(l.0);
            let inst = db as f64 / dt / 1048576.0;
            eprintln!(
                "[{:>5.1}s] inst={:>6.2} MB/s engine={:>6.2} MB/s conns={:>3} prog={:.1} dl={}",
                t0.elapsed().as_secs_f64(),
                inst,
                info.speed_bps as f64 / 1048576.0,
                info.active_conns,
                info.progress,
                info.downloaded
            );
            *l = (info.downloaded, Instant::now());
        }
    });

    let cancel_guard = cancel_flag.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        cancel_guard.store(true, Ordering::Relaxed);
    });

    let r = download_file(engine.clone(), cb).await;
    let total = engine.pool.file_size;
    let dl = engine.pool.total_downloaded();
    let el = t0.elapsed().as_secs_f64();
    match r {
        Ok(()) => eprintln!(
            "DONE elapsed={:.1}s total={} downloaded={} avg={:.2} MB/s",
            el,
            total,
            dl,
            dl as f64 / 1048576.0 / el.max(0.001)
        ),
        Err(e) => eprintln!("STOP elapsed={:.1}s downloaded={} err={}", el, dl, e),
    }
}
