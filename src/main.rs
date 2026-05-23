//! daman-bridge. The Daman ForagerBee.
//!
//! Bidirectional translator between the Daman copy-bond contract on
//! Arc and the hum mesh. One direction:
//!
//!   Arc events  -->  hum tones
//!     LeaderBondPosted     -> chi:"leader-bond-posted"
//!     FollowerSubscribed   -> chi:"follower-subscribed"
//!     TradeExecuted        -> chi:"trade-executed"
//!     SettlementCompleted  -> chi:"settlement-completed"
//!     DegradationFlagged   -> chi:"degradation-detected"
//!     DisputeOpened        -> chi:"dispute-opened"
//!     ArbiterRuled         -> chi:"ruling"
//!     BondSlashed          -> chi:"bond-slashed"
//!
//! Other direction:
//!
//!   hum tones  -->  Arc calls
//!     chi:"slash-claim"   -> attestDegradation(leader, evidenceHash)
//!     chi:"ruling"        -> arbiterRule(claimId, slashAmount, upheld)
//!
//! ADR-001 stays in force: the bridge is transport. Truth lives on
//! the chain. The bridge makes no judgments about which events deserve
//! mesh broadcast; it forwards all qualifying topics emitted by the
//! configured contract address.
//!
//! Chain writes use `eth_sendTransaction`, delegating signing to the
//! configured RPC (dev node with unlocked account or an authenticated
//! provider). Production deployments substitute a local signer.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info, warn};

const BEE_NAME: &str = "daman-bridge";
const BEE_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_RPC: &str = "https://rpc.testnet.arc.network";
const DEFAULT_POLL_MS: u64 = 4_000;

#[derive(Debug, Clone)]
struct Config {
    sock_path: String,
    rpc_url: String,
    copy_bond_addr: String,
    sender: String,
    poll_interval: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let runtime = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| {
            format!("/run/user/{}", unsafe { libc::geteuid() })
        });
        let default_sock = format!("{runtime}/hum/thrum.sock");
        Ok(Self {
            sock_path: std::env::var("HUM_THRUM_SOCK").unwrap_or(default_sock),
            rpc_url: std::env::var("DAMAN_BRIDGE_RPC").unwrap_or_else(|_| DEFAULT_RPC.into()),
            copy_bond_addr: std::env::var("DAMAN_COPY_BOND_ADDR")
                .context("DAMAN_COPY_BOND_ADDR is required")?,
            sender: std::env::var("DAMAN_BRIDGE_SENDER")
                .context("DAMAN_BRIDGE_SENDER is required: the EVM address the RPC will sign for")?,
            poll_interval: Duration::from_millis(
                std::env::var("DAMAN_BRIDGE_POLL_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_POLL_MS),
            ),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: Value,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    result: Option<T>,
    error: Option<Value>,
    #[allow(dead_code)]
    id: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RpcLog {
    #[allow(dead_code)]
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(rename = "blockNumber")]
    block_number: String,
    #[serde(rename = "transactionHash")]
    tx_hash: String,
}

/// Outbound chain calls the bridge issues on behalf of mesh tones.
#[derive(Debug, Clone)]
enum DispatchIntent {
    AttestDegradation { leader: String, evidence_hash: String, builder: String },
    ArbiterRule { claim_id: String, slash_amount: String, upheld: bool, builder: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Arc::new(Config::from_env()?);
    info!(
        rpc = %cfg.rpc_url,
        contract = %cfg.copy_bond_addr,
        sender = %cfg.sender,
        sock = %cfg.sock_path,
        "{BEE_NAME} starting"
    );

    let topics = topic_table();

    let (mesh_tx, mut mesh_rx) = mpsc::channel::<Value>(256);
    let (chain_tx, mut chain_rx) = mpsc::channel::<DispatchIntent>(256);

    // Mesh writer: connects to humd, handshakes, then forwards anything
    // that lands on mesh_rx. Also listens for slash-claim and ruling
    // chis from other bees, converting them into DispatchIntents.
    let cfg_mesh = cfg.clone();
    let chain_tx_for_mesh = chain_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = run_mesh_loop(cfg_mesh, &mut mesh_rx, chain_tx_for_mesh).await {
            warn!(error = %e, "mesh loop terminated");
        }
    });

    // Chain writer: drains chain_rx, issues eth_sendTransaction calls.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let cfg_writer = cfg.clone();
    let client_writer = client.clone();
    tokio::spawn(async move {
        while let Some(intent) = chain_rx.recv().await {
            if let Err(e) = dispatch_to_chain(&client_writer, &cfg_writer, &intent).await {
                warn!(error = %e, ?intent, "chain dispatch failed");
            }
        }
    });

    // Chain reader: polls eth_getLogs against the configured contract,
    // converts qualifying events into mesh tones.
    let mut cursor = fetch_block_number(&client, &cfg.rpc_url).await?;
    loop {
        match fetch_block_number(&client, &cfg.rpc_url).await {
            Ok(head) if head > cursor => {
                let from = cursor + 1;
                let to = head;
                match fetch_logs(&client, &cfg, from, to, &topics).await {
                    Ok(logs) => {
                        for log in logs {
                            if let Some(tone) = log_to_tone(&log, &topics) {
                                if let Err(e) = mesh_tx.send(tone).await {
                                    warn!(error = %e, "mesh channel closed");
                                }
                            }
                        }
                        cursor = to;
                    }
                    Err(e) => warn!(error = %e, "fetch_logs failed"),
                }
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "fetch_block_number failed"),
        }
        sleep(cfg.poll_interval).await;
    }
}

async fn run_mesh_loop(
    cfg: Arc<Config>,
    mesh_rx: &mut mpsc::Receiver<Value>,
    chain_tx: mpsc::Sender<DispatchIntent>,
) -> Result<()> {
    let stream = UnixStream::connect(&cfg.sock_path)
        .await
        .with_context(|| format!("connect to humd at {}", cfg.sock_path))?;
    let (read_half, write_half) = stream.into_split();
    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));

    let hello = json!({
        "chi": "hello",
        "bee": ["forager"],
        "chis": [
            "leader-bond-posted","follower-subscribed","trade-executed","settlement-completed",
            "degradation-detected","dispute-opened","ruling","bond-slashed",
            "slash-claim"
        ],
        "name": BEE_NAME,
        "version": BEE_VERSION,
    });
    {
        let mut w = write_half.lock().await;
        write_line(&mut *w, &hello).await?;
    }

    // Forward outbound mesh tones produced by the chain reader.
    let write_for_forward = write_half.clone();
    tokio::spawn(async move {
        // mesh_rx is owned by the outer task; can't move it here. The
        // outer task drains it via the loop below. Placeholder split.
        // (kept minimal: see the loop below.)
        let _ = write_for_forward;
    });

    let mut reader = BufReader::new(read_half).lines();

    loop {
        tokio::select! {
            Some(tone) = mesh_rx.recv() => {
                let mut w = write_half.lock().await;
                if let Err(e) = write_line(&mut *w, &tone).await {
                    warn!(error = %e, "mesh write failed");
                }
            }
            line = reader.next_line() => {
                match line {
                    Ok(Some(s)) if !s.trim().is_empty() => {
                        if let Some(intent) = parse_inbound_dispatch(&s) {
                            if let Err(e) = chain_tx.send(intent).await {
                                warn!(error = %e, "chain channel closed");
                            }
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => return Err(anyhow!("humd socket closed")),
                    Err(e) => return Err(anyhow!("humd read failed: {e}")),
                }
            }
        }
    }
}

fn parse_inbound_dispatch(line: &str) -> Option<DispatchIntent> {
    let v: Value = serde_json::from_str(line).ok()?;
    let chi = v.get("chi")?.as_str()?;
    let args = v.get("args")?;
    match chi {
        "slash-claim" => Some(DispatchIntent::AttestDegradation {
            leader: args.get("leader")?.as_str()?.to_string(),
            evidence_hash: args.get("evidenceHash")?.as_str()?.to_string(),
            builder: args
                .get("builder")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(zero_bytes32),
        }),
        "ruling" => Some(DispatchIntent::ArbiterRule {
            claim_id: args.get("claimId")?.as_str()?.to_string(),
            slash_amount: args.get("slashAmount")?.as_str()?.to_string(),
            upheld: args.get("upheld")?.as_bool()?,
            builder: args
                .get("builder")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(zero_bytes32),
        }),
        _ => None,
    }
}

async fn dispatch_to_chain(
    client: &reqwest::Client,
    cfg: &Config,
    intent: &DispatchIntent,
) -> Result<()> {
    let calldata = match intent {
        DispatchIntent::AttestDegradation { leader, evidence_hash, builder } => {
            // attestDegradation(address,bytes32,bytes32)
            let selector = keccak_selector("attestDegradation(address,bytes32,bytes32)");
            let leader_word = pad_left_address(leader);
            let evidence_word = pad_left_word(strip_hex(evidence_hash));
            let builder_word = pad_left_word(strip_hex(builder));
            format!("0x{}{}{}{}", selector, leader_word, evidence_word, builder_word)
        }
        DispatchIntent::ArbiterRule { claim_id, slash_amount, upheld, builder } => {
            // arbiterRule(uint256,uint256,bool,bytes32)
            let selector = keccak_selector("arbiterRule(uint256,uint256,bool,bytes32)");
            let claim_word = pad_left_word(strip_hex(claim_id));
            let amount_word = pad_left_word(strip_hex(slash_amount));
            let upheld_word = if *upheld {
                "0".repeat(63) + "1"
            } else {
                "0".repeat(64)
            };
            let builder_word = pad_left_word(strip_hex(builder));
            format!("0x{}{}{}{}{}", selector, claim_word, amount_word, upheld_word, builder_word)
        }
    };

    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "eth_sendTransaction",
        params: json!([{
            "from": cfg.sender,
            "to": cfg.copy_bond_addr,
            "data": calldata,
        }]),
        id: 1,
    };
    let resp: JsonRpcResponse<String> = client
        .post(&cfg.rpc_url)
        .json(&req)
        .send()
        .await?
        .json()
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("rpc error: {}", err));
    }
    let tx_hash = resp.result.ok_or_else(|| anyhow!("missing tx hash"))?;
    info!(?intent, tx = %tx_hash, "dispatched");
    Ok(())
}

async fn write_line<W: AsyncWriteExt + Unpin>(w: &mut W, v: &Value) -> Result<()> {
    let s = serde_json::to_string(v)?;
    w.write_all(s.as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}

async fn fetch_block_number(client: &reqwest::Client, rpc_url: &str) -> Result<u64> {
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "eth_blockNumber",
        params: json!([]),
        id: 1,
    };
    let resp: JsonRpcResponse<String> = client.post(rpc_url).json(&req).send().await?.json().await?;
    if let Some(e) = resp.error {
        return Err(anyhow!("rpc error: {}", e));
    }
    let hex = resp.result.ok_or_else(|| anyhow!("missing block number"))?;
    parse_hex_u64(&hex)
}

async fn fetch_logs(
    client: &reqwest::Client,
    cfg: &Config,
    from_block: u64,
    to_block: u64,
    topics: &TopicTable,
) -> Result<Vec<RpcLog>> {
    let topic0_list: Vec<&str> = topics.by_hash.keys().map(|k| k.as_str()).collect();
    let filter = json!([{
        "fromBlock": format!("0x{:x}", from_block),
        "toBlock": format!("0x{:x}", to_block),
        "address": cfg.copy_bond_addr,
        "topics": [topic0_list]
    }]);
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "eth_getLogs",
        params: filter,
        id: 2,
    };
    let resp: JsonRpcResponse<Vec<RpcLog>> =
        client.post(&cfg.rpc_url).json(&req).send().await?.json().await?;
    if let Some(e) = resp.error {
        return Err(anyhow!("rpc error: {}", e));
    }
    Ok(resp.result.unwrap_or_default())
}

struct TopicTable {
    by_hash: HashMap<String, &'static str>,
}

fn topic_table() -> TopicTable {
    let pairs: &[(&str, &str)] = &[
        ("leader-bond-posted", "LeaderBondPosted(address,uint256,uint256)"),
        ("follower-subscribed", "FollowerSubscribed(address,address,uint256,bytes32)"),
        ("trade-executed", "TradeExecuted(address,address,uint256,bool,uint64)"),
        ("settlement-completed", "SettlementCompleted(address,uint256,int256,uint64)"),
        ("degradation-detected", "DegradationFlagged(uint256,address,address,bytes32,bytes32)"),
        ("dispute-opened", "DisputeOpened(uint256,address)"),
        ("ruling", "ArbiterRuled(uint256,uint256,bool,bytes32)"),
        ("bond-slashed", "BondSlashed(address,uint256,uint256)"),
    ];
    let mut by_hash = HashMap::new();
    for (chi, sig) in pairs {
        by_hash.insert(format!("0x{}", keccak_hex(sig.as_bytes())), *chi);
    }
    TopicTable { by_hash }
}

fn log_to_tone(log: &RpcLog, topics: &TopicTable) -> Option<Value> {
    let topic0 = log.topics.first()?;
    let chi = topics.by_hash.get(topic0)?;
    let block_number = parse_hex_u64(&log.block_number).ok()?;
    Some(json!({
        "chi": chi,
        "args": {
            "topics": log.topics,
            "data": log.data,
            "blockNumber": block_number,
            "txHash": log.tx_hash,
        }
    }))
}

fn keccak_selector(signature: &str) -> String {
    let full = keccak_hex(signature.as_bytes());
    full[..8].to_string()
}

fn keccak_hex(bytes: &[u8]) -> String {
    let mut h = Keccak256::new();
    h.update(bytes);
    let digest = h.finalize();
    hex::encode(digest)
}

fn pad_left_address(addr: &str) -> String {
    let s = strip_hex(addr);
    if s.len() >= 40 {
        format!("{}{}", "0".repeat(24), &s[s.len() - 40..])
    } else {
        format!("{}{}", "0".repeat(64 - s.len()), s)
    }
}

fn pad_left_word(s: String) -> String {
    if s.len() >= 64 {
        s
    } else {
        format!("{}{}", "0".repeat(64 - s.len()), s)
    }
}

fn strip_hex(s: &str) -> String {
    s.trim_start_matches("0x").to_string()
}

fn zero_bytes32() -> String {
    format!("0x{}", "0".repeat(64))
}

fn parse_hex_u64(s: &str) -> Result<u64> {
    let stripped = s.trim_start_matches("0x");
    u64::from_str_radix(stripped, 16).context("parse u64 from hex")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_matches_known_signature() {
        // transfer(address,uint256) is well-known: a9059cbb
        let sel = keccak_selector("transfer(address,uint256)");
        assert_eq!(sel, "a9059cbb");
    }

    #[test]
    fn topic_table_contains_known_signatures() {
        let t = topic_table();
        // Check that all 8 mappings are present.
        assert_eq!(t.by_hash.len(), 8);
    }

    #[test]
    fn pad_left_address_pads_to_32_bytes() {
        let padded = pad_left_address("0xabcdef0123456789abcdef0123456789abcdef01");
        assert_eq!(padded.len(), 64);
        assert!(padded.ends_with("abcdef0123456789abcdef0123456789abcdef01"));
    }

    #[test]
    fn parse_inbound_dispatch_handles_slash_claim() {
        let raw = r#"{"chi":"slash-claim","args":{"leader":"0xabc","evidenceHash":"0xdef","builder":"0xf00d"}}"#;
        let intent = parse_inbound_dispatch(raw).unwrap();
        match intent {
            DispatchIntent::AttestDegradation { leader, evidence_hash, builder } => {
                assert_eq!(leader, "0xabc");
                assert_eq!(evidence_hash, "0xdef");
                assert_eq!(builder, "0xf00d");
            }
            _ => panic!("expected AttestDegradation"),
        }
    }
}
