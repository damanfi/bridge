# daman-bridge

The Daman ForagerBee for hum. Bidirectional translator between the Daman copy-bond contract on Arc and the hum mesh.

## Wire

```
Arc events                    hum tones
─────────────────────────     ─────────────────────────
LeaderBondPosted          ──> chi:"leader-bond-posted"
FollowerSubscribed        ──> chi:"follower-subscribed"
TradeExecuted             ──> chi:"trade-executed"
SettlementCompleted       ──> chi:"settlement-completed"
DegradationFlagged        ──> chi:"degradation-detected"
DisputeOpened             ──> chi:"dispute-opened"
ArbiterRuled              ──> chi:"ruling"
BondSlashed               ──> chi:"bond-slashed"

hum tones                     Arc calls
─────────────────────────     ─────────────────────────
chi:"slash-claim"         ──> attestDegradation(leader, evidenceHash)
chi:"ruling"              ──> arbiterRule(claimId, slashAmount, upheld)
```

## ADR-001

The bridge is transport. The chain is truth. The bridge makes no judgments about which events to broadcast; it forwards all qualifying topics emitted by the configured contract address.

## Propensity

| statefulness | richness | wire shape | hides |
|---|---|---|---|
| stateless (per-event) | thin (event-to-tone translation) | bidirectional | everything except the documented chis |

## Configure

| env | required | default | what |
|---|---|---|---|
| `DAMAN_COPY_BOND_ADDR` | yes | none | deployed `IDamanCopyBond` contract address |
| `DAMAN_BRIDGE_SENDER` | yes | none | EVM address the RPC will sign for |
| `DAMAN_BRIDGE_RPC` | no | `https://rpc.testnet.arc.network` | JSON-RPC endpoint |
| `DAMAN_BRIDGE_POLL_MS` | no | `4000` | chain poll interval in ms |
| `HUM_THRUM_SOCK` | no | `$XDG_RUNTIME_DIR/hum/thrum.sock` | humd's NDJSON socket |

## Run

```bash
DAMAN_COPY_BOND_ADDR=0x... \
DAMAN_BRIDGE_SENDER=0x... \
  cargo run --release
```

The bee self-announces on connection. Chain reads start from the current head; back-fill is left to operators if they need it.

## Signing

`dispatch_to_chain` issues `eth_sendTransaction`, delegating signing to the configured RPC. This works against a dev node with an unlocked account, or an authenticated provider. Production deployments substitute a local signer (alloy / ethers-rs / a hardware signer) without changing the rest of the bee.

## What it doesn't do

No event back-fill. No on-mesh routing logic beyond the documented topic table. No retry policy on failed `eth_sendTransaction`. These are reference-implementation gaps deliberately left for the operator.

## License

Apache-2.0.
