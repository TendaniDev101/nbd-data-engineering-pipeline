# Stream Data (Stage 3)

Place the 12 stream batch files here for local testing.

These files are provided in the Stage 3 data release. Copy them into this directory:

```
stream/
├── stream_20260320_143000_0001.jsonl
├── stream_20260320_143500_0002.jsonl
├── ...
└── stream_20260320_152500_0012.jsonl
```

During scoring, this directory is mounted read-only at `/data/stream/` inside the Docker container.

**Important:** Do NOT commit the stream data files to your repository — only the `.gitkeep` and this README should be committed. The scoring system provides the stream data via Docker mount.
