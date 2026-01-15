#!/bin/bash
# Submit test_pipeline_normalization.py to Anyscale cluster

cd "$(dirname "$0")/.."

anyscale job submit \
  --name pipeline-test \
  --wait \
  --working-dir . \
  -- python tests/test_pipeline_normalization.py
