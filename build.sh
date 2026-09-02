#!/usr/bin/env bash
set -euo pipefail

function_dir="functions/order_processor"
build_dir="$function_dir/build"

rm -rf "$build_dir"
mkdir -p "$build_dir"
python3.13 -m pip install --quiet --target "$build_dir" -r "$function_dir/requirements.txt"
cp "$function_dir/handler.py" "$build_dir/"
echo "Lambda package ready in $build_dir"
