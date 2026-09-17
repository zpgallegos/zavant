#!/usr/bin/env bash
# Called by systemd after loading the shared /etc/zavant/dagster.env file.
set -euo pipefail
cd /opt/zavant/app
case "${1:-}" in
  prepare)
    exec .venv/bin/python -m zavant.orchestration.prepare ;;
  code)
    exec .venv/bin/dagster api grpc -h 127.0.0.1 -p 4000 \
      -m zavant.orchestration.definitions ;;
  webserver)
    exec .venv/bin/dagster-webserver -h 127.0.0.1 -p 3000 \
      -w infrastructure/dagster/workspace.yaml ;;
  daemon)
    exec .venv/bin/dagster-daemon run -w infrastructure/dagster/workspace.yaml ;;
  *) echo "Expected prepare, code, webserver, or daemon." >&2; exit 2 ;;
esac
