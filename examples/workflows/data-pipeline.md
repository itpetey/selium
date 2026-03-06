# Data Pipeline Workflow

This rewrites the legacy `examples/data-pipeline` flow for the new control-plane model.

## 1. Bootstrap and publish contract

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 idl publish --input examples/idl/data_pipeline_v1.selium
```

## 2. Deploy generator, transform, and sink apps

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 deploy \
  --app generator --module modules/generator.wasm \
  --contract analytics.pipeline/ingest.numbers@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 deploy \
  --app transformer --module modules/transformer.wasm \
  --contract analytics.pipeline/ingest.numbers@v1 \
  --contract analytics.pipeline/transform.summaries@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 deploy \
  --app sink --module modules/sink.wasm \
  --contract analytics.pipeline/transform.summaries@v1
```

## 3. Wire edges and reconcile

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 connect \
  --pipeline pipeline --namespace analytics \
  --from-app generator --to-app transformer \
  --contract analytics.pipeline/ingest.numbers@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 connect \
  --pipeline pipeline --namespace analytics \
  --from-app transformer --to-app sink \
  --contract analytics.pipeline/transform.summaries@v1

cargo run -p selium -- --daemon-addr 127.0.0.1:7100 agent --once
```
