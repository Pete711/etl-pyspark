#!/bin/bash
# This script tests the entire etl-pyspark project locally, injecting environment variables
# Run from the test/ directory in the project root

# Exit on error
set -e

# Set project root to parent of test/
PROJECT_ROOT="$(realpath "$(dirname "$(pwd)")")"

# Move up to the project root
cd ..
PROJECT_ROOT="$(pwd)"

# Validate script paths
SLICER_SCRIPT="${PROJECT_ROOT}/parquet-slicer-v1.0/app/parquet_slicer.py"
ETL_SCRIPT="${PROJECT_ROOT}/etl-pyspark-v1.0/app/main.py"
ASSEMBLER_SCRIPT="${PROJECT_ROOT}/parquet-assembler-v1.0/app/parquet_assembler.py"

for script in "${SLICER_SCRIPT}" "${ETL_SCRIPT}" "${ASSEMBLER_SCRIPT}"; do
    if [ ! -f "${script}" ]; then
        echo "Error: Script not found: ${script}"
        exit 1
    fi
done

# Define directories
SOURCE_DIR="${PROJECT_ROOT}/source_dir"
CHUNKS_DIR="${PROJECT_ROOT}/chunks_dir"
TASK_RESULTS_DIR="${PROJECT_ROOT}/task_results_dir"
OUTPUT_DIR="${PROJECT_ROOT}/output_dir"

# Create and set permissions for directories
for dir in "${SOURCE_DIR}" "${CHUNKS_DIR}" "${TASK_RESULTS_DIR}" "${OUTPUT_DIR}"; do
    if [ ! -d "${dir}" ]; then
        echo "Creating directory: ${dir}"
        mkdir -p "${dir}"
    fi
    chmod u+rwx "${dir}"
done

# Set environment variables
## Partitioner environment variables
export DATA_URL="https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet"
export CHUNK_SIZE=5000
export SLICE_DIR="${CHUNKS_DIR}"
export PARTITIONER_PARAMS='{}'

## App environment variables
export TASK_DIR="${CHUNKS_DIR}"
export INPUT_DIR="${SOURCE_DIR}"
export TASK_RESULTS_DIR="${TASK_RESULTS_DIR}"
export OUTPUT_DIR="${OUTPUT_DIR}"
export APP_PARAMS='{"chunk_size": 5000, "dataset_url": "https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet", "num_threads": 4}'

## Assembler environment variables
export RESULTS_DIR="${TASK_RESULTS_DIR}"
export OUTPUT_PATH="${OUTPUT_DIR}/result.parquet"
export ASSEMBLER_PARAMS='{}'

# Set PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/parquet-slicer-v1.0/app:${PROJECT_ROOT}/parquet-assembler-v1.0/app:${PROJECT_ROOT}/etl-pyspark-v1.0/app:${PYTHONPATH}"

# Log environment setup
echo "Testing etl-pyspark pipeline locally"
echo "Project root: ${PROJECT_ROOT}"
echo "Python version: $(python3 --version)"
echo "Running with environment variables:"
echo "  DATA_URL=${DATA_URL}"
echo "  CHUNK_SIZE=${CHUNK_SIZE}"
echo "  SLICE_DIR=${SLICE_DIR}"
echo "  INPUT_DIR=${INPUT_DIR}"
echo "  TASK_DIR=${TASK_DIR}"
echo "  TASK_RESULTS_DIR=${TASK_RESULTS_DIR}"
echo "  OUTPUT_DIR=${OUTPUT_DIR}"
echo "  OUTPUT_PATH=${OUTPUT_PATH}"
echo "  PYTHONPATH=${PYTHONPATH}"

# Clear existing directories
echo "Clearing directories..."
rm -rf "${CHUNKS_DIR}/*" "${SOURCE_DIR}/*" "${TASK_RESULTS_DIR}/*" "${OUTPUT_DIR}/*" 2>/dev/null || true

# Run slicer and check output
echo "Running parquet slicer..."
python3 "${SLICER_SCRIPT}"
if [ -z "$(ls -A "${CHUNKS_DIR}")" ]; then
    echo "Error: No slice files generated in ${CHUNKS_DIR}"
    exit 1
fi
echo "Slice files generated:"
ls "${CHUNKS_DIR}"

# Run ETL and check output
echo "Running ETL orchestrator..."
python3 "${ETL_SCRIPT}"
if [ -z "$(ls -A "${TASK_RESULTS_DIR}")" ]; then
    echo "Error: No result files generated in ${TASK_RESULTS_DIR}"
    exit 1
fi
echo "Result files generated:"
ls "${TASK_RESULTS_DIR}"

# Run assembler
echo "Running parquet assembler..."
python3 "${ASSEMBLER_SCRIPT}"
if [ ! -f "${OUTPUT_DIR}/result.parquet" ]; then
    echo "Error: No output file generated in ${OUTPUT_DIR}/result.parquet"
    exit 1
fi
echo "Output file generated: ${OUTPUT_DIR}/result.parquet"

echo "Pipeline completed successfully. Output: ${OUTPUT_DIR}/result.parquet"

# Clean up environment variables
unset DATA_URL CHUNK_SIZE SLICE_DIR INPUT_DIR TASK_DIR TASK_RESULTS_DIR OUTPUT_DIR OUTPUT_PATH PARTITIONER_PARAMS APP_PARAMS ASSEMBLER_PARAMS
export PYTHONPATH="${PYTHONPATH#${PROJECT_ROOT}/parquet-slicer-v1.0/app:${PROJECT_ROOT}/parquet-assembler-v1.0/app:${PROJECT_ROOT}/etl-pyspark-v1.0/app:}"

echo "Environment variables cleaned up."
cd test