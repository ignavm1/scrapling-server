#!/usr/bin/env bash
# Corre la medicion del angulo LinkedIn con el interprete del venv del repo:
# el python del sistema no trae scrapling, y ese fallo se leeria como "el
# angulo no sirve".
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
if [ ! -x "$REPO/.venv/bin/python" ]; then
  echo "Falta $REPO/.venv/bin/python: crea el venv antes de medir." >&2
  exit 1
fi
exec "$REPO/.venv/bin/python" "$REPO/bench/medir_linkedin.py"
