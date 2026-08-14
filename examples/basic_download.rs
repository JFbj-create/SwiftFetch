//! SwiftFetch v3 插件化解耦版 示例: 调用库下载文件，带进度回调
//!
//! 说明: 插件化解耦架构升级 100% 保留 SwiftFetch::download() 外部 API。
//!       内部调度从 "模块 trait 直接函数调用" 改为 "插件 + 消息总线",
//!       业务逻辑(speed_engine.rs / bt_engine.rs) 完全不动, 仅外层调度薄包装。
//!
//! 运行: cargo run --example basic_download -- <URL> [output_path]

use std::path::PathBuf;
use std::time::Instant;
use swiftfetch::{
    DownloadConfig, ProgressInfo, SwiftFetch,
    format_bytes, format_progress_bar, format_speed,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let url = if !args.is_empty() {
        args[0].clone()
    } else {
        eprintln!("用法: cargo run --example basic_download -- <URL> [output_path]");
        eprintln!("示例 URL: https://speed.cloudflare.com/__down?bytes=10485760");
        std::process::exit(2);
    };

    let output = args.get(1).map(PathBuf::from);

    println!("╔══════════════════════════════════════════════════╗");
    println!("║   SwiftFetch v3 - 插件化解耦版 示例程序          ║");
    println!("║   内核: 插件+消息总线 | 双模式 Thread/Process    ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!();

    let cfg = DownloadConfig {
        url: url.clone(),
        output,
        connections: 16,
        base_chunk_size: None,
        auto_adjust: true,
        resume_enabled: true,
        proxy: None,
        headers: DownloadConfig::default_headers(),
        timeout_connect: std::time::Duration::from_secs(10),
        timeout_read: std::time::Duration::from_secs(180),
        timeout_request: std::time::Duration::from_secs(300),
    };

    println!("URL     : {}", &url[..url.len().min(80)]);
    println!("并发    : {}", cfg.connections);
    println!("断点续传: 开");
    println!();

    let sf = SwiftFetch::new(cfg);
    let start = Instant::now();
    let last_line = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let last_line_clone = last_line.clone();

    let result = sf
        .download(move |info: ProgressInfo| {
            let bar = format_progress_bar(info.progress, 25);
            let speed = format_speed(info.speed_bps);
            let dl = format_bytes(info.downloaded);
            let total = format_bytes(info.total);
            let eta = info
                .eta_sec
                .map(|s| format!("{}:{:02}", s / 60, s % 60))
                .unwrap_or_else(|| "--:--".to_string());
            let slow = if info.slow_bases > 0 {
                format!(" 慢块={}", info.slow_bases)
            } else {
                String::new()
            };
            let line = format!(
                "\r {} {:>5.1}% | {:>11} | {}/{} | conn={:>2} | ETA {}{}   ",
                bar, info.progress, speed, dl, total, info.active_conns, eta, slow
            );
            let mut stored = last_line_clone.lock().unwrap();
            if *stored != line {
                *stored = line.clone();
                let line = if info.progress >= 100.0 {
                    format!("{}\n", line.trim_end())
                } else {
                    line
                };
                let _ = std::io::Write::write_all(&mut std::io::stdout(), line.as_bytes());
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
        })
        .await?;

    let elapsed = start.elapsed().as_secs_f64();
    println!();
    println!("┌──────────────────────────────────────────────────┐");
    println!("│ ✓ 下载完成                                       │");
    println!("│ 输出文件 : {:<40} │", truncate(&result.output_path.display().to_string(), 40));
    println!("│ 文件大小 : {:<40} │", format_bytes(result.file_size));
    println!("│ 平均速度 : {:<40} │", format_speed(result.avg_speed_bps));
    println!("│ 总用时   : {:<40.3}s│", elapsed);
    println!("└──────────────────────────────────────────────────┘");

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max.saturating_sub(3)).collect();
        t.push_str("...");
        t
    }
}
