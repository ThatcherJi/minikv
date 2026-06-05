use std::io::{self, Write};

use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use reqwest::Client;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7000")]
    coord: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Put { key: String, value: String },
    Get { key: String },
    Del { key: String },
    Status,
    Cluster,
    Ring { key: String },
    VolumeStats,
    Health { addr: String },
    Keys(KeysArgs),
    Compact { addr: String },
}

#[derive(Debug, ClapArgs)]
struct KeysArgs {
    addr: String,
    #[arg(long)]
    prefix: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::new();
    let coord = args.coord.trim_end_matches('/').to_string();

    match args.command {
        Command::Put { key, value } => {
            let url = format!("{coord}/kv/{}", encode_path_segment(&key));
            let resp = client
                .put(url)
                .body(value.into_bytes())
                .send()
                .await
                .context("put request failed")?;
            ensure_success(resp.status(), "put")?;
            println!("ok");
        }
        Command::Get { key } => {
            let url = format!("{coord}/kv/{}", encode_path_segment(&key));
            let resp = client.get(url).send().await.context("get request failed")?;
            ensure_success(resp.status(), "get")?;
            let bytes = resp.bytes().await.context("failed to read get response")?;
            io::stdout()
                .write_all(&bytes)
                .context("failed to write value to stdout")?;
            println!();
        }
        Command::Del { key } => {
            let url = format!("{coord}/kv/{}", encode_path_segment(&key));
            let resp = client
                .delete(url)
                .send()
                .await
                .context("del request failed")?;
            ensure_success(resp.status(), "del")?;
            println!("ok");
        }
        Command::Status => {
            let url = format!("{coord}/status");
            print_json_get(&client, url, "status").await?;
        }
        Command::Cluster => {
            let url = format!("{coord}/admin/cluster");
            print_json_get(&client, url, "cluster").await?;
        }
        Command::Ring { key } => {
            let url = format!("{coord}/admin/ring/{}", encode_path_segment(&key));
            print_json_get(&client, url, "ring").await?;
        }
        Command::VolumeStats => {
            let url = format!("{coord}/admin/volumes/stats");
            print_json_get(&client, url, "volume-stats").await?;
        }
        Command::Health { addr } => {
            let url = format!("{}/healthz", normalize_base_url(&addr));
            let resp = client
                .get(url)
                .send()
                .await
                .context("health request failed")?;
            ensure_success(resp.status(), "health")?;
            let body = resp
                .text()
                .await
                .context("failed to read health response")?;
            println!("{body}");
        }
        Command::Keys(args) => {
            let mut url = format!("{}/admin/keys", normalize_base_url(&args.addr));
            let mut query = Vec::new();
            if let Some(prefix) = args.prefix {
                query.push(format!("prefix={}", encode_query_component(&prefix)));
            }
            if let Some(limit) = args.limit {
                query.push(format!("limit={limit}"));
            }
            if !query.is_empty() {
                url.push('?');
                url.push_str(&query.join("&"));
            }
            print_json_get(&client, url, "keys").await?;
        }
        Command::Compact { addr } => {
            let url = format!("{}/admin/compact", normalize_base_url(&addr));
            print_json_post(&client, url, "compact").await?;
        }
    }

    Ok(())
}

async fn print_json_get(client: &Client, url: String, operation: &str) -> Result<()> {
    let value = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("{operation} request failed"))?
        .error_for_status()
        .with_context(|| format!("{operation} returned error"))?
        .json::<serde_json::Value>()
        .await
        .with_context(|| format!("failed to decode {operation} json"))?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

async fn print_json_post(client: &Client, url: String, operation: &str) -> Result<()> {
    let value = client
        .post(url)
        .send()
        .await
        .with_context(|| format!("{operation} request failed"))?
        .error_for_status()
        .with_context(|| format!("{operation} returned error"))?
        .json::<serde_json::Value>()
        .await
        .with_context(|| format!("failed to decode {operation} json"))?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn ensure_success(status: reqwest::StatusCode, operation: &str) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        anyhow::bail!("{operation} returned {status}")
    }
}

fn normalize_base_url(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

fn encode_query_component(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push('+'),
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
}

fn encode_path_segment(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
}
