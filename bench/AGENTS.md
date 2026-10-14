# AGENTS.md

- `bench/requirements.md` is the source of truth for the benchmark harness, its remote target, and the AWS Lakekeeper benchmark catalog. Read it before changing `bench/` or the `deploy/` scripts it covers.
- A stray `bench/.env` redirects `make bench` and `bench/run.sh` to a remote target, and `BENCH_TARGET=docker` alone does not undo it. Before debugging a hung bench run, move `bench/.env` aside.
