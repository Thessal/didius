#!/bin/bash
for FILE in ./data/{articles,wikipedia}/*/*; do \
  python summary.py \
  --input_file "$FILE" \
  --prompt_file "./prompts/summary.json" \
  --endpoint "http://192.168.0.23:11434" \
#   --endpoint "http://localhost:11434" \
  --model "gpt-oss:120b" \
  --metadata_dir "./metadata" \
  --output_dir "./data/summary" \
  --chunk_size 3000 \
  --overlap 300 \
  ; done