# Orchestrator Workflow

This is the control-plane rewrite of legacy `examples/orchestrator`.

## 1. Publish orchestration contract

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 idl publish --input examples/idl/orchestrator_v1.selium
```

## 2. Deploy orchestrator and worker

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 deploy \
  --app orchestrator --module modules/orchestrator.wasm \
  --contract orchestration.worker/orchestrator.config@v1 \
  --contract orchestration.worker/orchestrator.tasks@v1 \
  --contract orchestration.worker/orchestrator.results@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 deploy \
  --app worker --module modules/worker.wasm \
  --contract orchestration.worker/orchestrator.config@v1 \
  --contract orchestration.worker/orchestrator.tasks@v1 \
  --contract orchestration.worker/orchestrator.results@v1
```

## 3. Connect channels and run reconcile

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 connect \
  --pipeline orchestrator --namespace orchestration \
  --from-app orchestrator --to-app worker \
  --contract orchestration.worker/orchestrator.tasks@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 connect \
  --pipeline orchestrator --namespace orchestration \
  --from-app worker --to-app orchestrator \
  --contract orchestration.worker/orchestrator.results@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 agent --once
```
