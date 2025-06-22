#!/bin/bash
# This script tests the whole etl-pyspark project locally, injecting the environment variables
 
# Set the environment variables

## Partitioner environment variables
export SOURCE_DIR='./source_dir/'
export CHUNKS_DIR='./chunks_dir/'
export PARTITIONER_PARAMS='{}'

## App environment variables
export TASK_DIR='./chunks_dir/'
export TASK_RESULTS_DIR='./task_results_dir/'
export APP_PARAMS='{}'
export CHUNK_NUMBER=0

## Assembler environment variables
# export TASK_RESULTS_DIR='./task_results_dir/' 
export OUTPUT_DIR='./output_dir/'
export ASSEMBLER_PARAMS='{}'

# Launch test scripts
# ... python3 ../etl-pyspark-v1.0/app/main.py ...
