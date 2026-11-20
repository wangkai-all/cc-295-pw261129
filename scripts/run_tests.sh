#!/usr/bin/env bash
# 跑全部离线测试（不需要起 Web 服务）。使用 -I 隔离模式，避免 cwd 注入。
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -I tests/test_engine.py
python3 -I tests/test_state.py
echo "全部离线测试通过"
