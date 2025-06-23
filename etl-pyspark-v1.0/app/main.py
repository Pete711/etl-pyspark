import json
import os
import sys
import logging
import shutil
import platform
from multiprocessing import Pool
from pathlib import Path
import subprocess
import multiprocessing as mp

# Add component app directories to sys.path
project_root = str(Path(os.path.dirname(__file__)).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'parquet-slicer-v1.0', 'app'))
sys.path.insert(0, os.path.join(project_root, 'parquet-assembler-v1.0', 'app'))

from parquet_slicer import partitioner
from parquet_assembler import assembler
from nyc_taxi_etl import run

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s")
logger = logging.getLogger(__name__)

# Default paths (relative to project root)
SLICES_DIR = "chunks_dir"
INPUT_DIR = "source_dir"
RESULTS_DIR = "task_results_dir"
OUTPUT_DIR = "output_dir"

# Default parameters and metadata
DEFAULT_PARAMS = {
    "chunk_size": 5000,
    "dataset_url": "https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet",
    "num_threads": min(mp.cpu_count(), 4)
}

DEFAULT_METADATA = {
    "job_name": "nyc-taxi-etl",
    "version": "1.0",
    "created_at": "2025-06-20T23:05:00Z"
}

def ensure_file_exists(filepath: str, default_content: dict) -> str:
    filepath = str(Path(filepath))
    if not os.path.exists(filepath):
        logger.info(f"File {filepath} not found. Creating with default content.")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(default_content, f, indent=4)
    with open(filepath) as f:
        return f.read()

def clear_directory(dir_path: str) -> None:
    dir_path = str(Path(dir_path))
    if os.path.exists(dir_path):
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to remove {file_path}: {e}")
        logger.debug(f"Cleared directory: {dir_path}")

def check_environment() -> None:
    logger.debug(f"Environment variables: {dict(os.environ)}")
    for var in ['BYTENITE_API_KEY', 'TASK_ID', 'INPUT_PATH', 'OUTPUT_PATH']:
        if var in os.environ:
            logger.warning(f"Found distributed env var {var}={os.environ[var]}. Unsetting for local run.")
            del os.environ[var]
    system = platform.system().lower()
    try:
        if system == 'windows':
            result = subprocess.run(['tasklist'], capture_output=True, text=True)
            if 'java.exe' in result.stdout.lower():
                logger.warning("Found running Java processes. Terminating them...")
                subprocess.run(['taskkill', '/F', '/IM', 'java.exe'], capture_output=True, text=True)
        else:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            if 'spark' in result.stdout.lower() or 'java' in result.stdout.lower():
                logger.warning("Found running Spark/Java processes. Terminating them...")
                subprocess.run(['killall', 'java'], capture_output=True, text=True)
                subprocess.run(['killall', 'spark'], capture_output=True, text=True, check=False)
    except Exception as e:
        logger.error(f"Failed to check or terminate processes: {e}")

def process_chunk(args):
    slice_file, index, slices_dir, input_dir, results_dir = args
    logger.info(f"Processing slice {index + 1}: {slice_file}")
    input_path = os.path.join(input_dir, f'data_{index}.bin')
    slice_path = os.path.join(slices_dir, slice_file)
    try:
        shutil.copy(slice_path, input_path)
        logger.debug(f"Copied {slice_path} to {input_path}")
        run(input_path=input_path, task_id=str(index), output_dir=results_dir)
        return True
    except Exception as e:
        logger.error(f"Failed to process slice {slice_file}: {e}")
        raise
    finally:
        if os.path.exists(input_path):
            try:
                os.remove(input_path)
                logger.debug(f"Removed temporary input file: {input_path}")
            except Exception as e:
                logger.warning(f"Failed to remove {input_path}: {e}")

def main():
    basepath = str(Path(os.path.dirname(__file__)).parent.parent)
    logger.info(f"Using base path: {basepath}")
    check_environment()
    params_path = os.path.join(basepath, 'params.json')
    metadata_path = os.path.join(basepath, 'metadata.json')
    params_content = ensure_file_exists(params_path, DEFAULT_PARAMS)
    metadata_content = ensure_file_exists(metadata_path, DEFAULT_METADATA)
    params = json.loads(params_content)
    metadata = json.loads(metadata_content)
    if 'num_threads' not in params:
        logger.warning(f"'num_threads' not found in params.json. Using default: {DEFAULT_PARAMS['num_threads']}")
        params['num_threads'] = DEFAULT_PARAMS['num_threads']
    logger.info(f"Task parameters: {params}")
    logger.info(f"Task metadata: {metadata}")
    try:
        for dir_path in [SLICES_DIR, INPUT_DIR, RESULTS_DIR, OUTPUT_DIR]:
            dir_path = os.path.join(basepath, dir_path)
            os.makedirs(dir_path, exist_ok=True)
            clear_directory(dir_path)
            logger.debug(f"Created/cleared directory: {dir_path}")
        logger.info(f"Partitioning dataset from {params['dataset_url']} into slices...")
        partitioner(
            data_url=params['dataset_url'],
            chunk_size=params['chunk_size'],
            output_dir=os.path.join(basepath, SLICES_DIR)
        )
        slices_dir = os.path.join(basepath, SLICES_DIR)
        slice_files = [f for f in os.listdir(slices_dir) if f.endswith('.parquet')]
        if not slice_files:
            logger.error("No slices found in slices directory.")
            raise ValueError("No slices found to process.")
        logger.info(f"Found {len(slice_files)} slices to process with {params['num_threads']} threads.")
        chunk_args = [
            (slice_file, i, slices_dir, os.path.join(basepath, INPUT_DIR), os.path.join(basepath, RESULTS_DIR))
            for i, slice_file in enumerate(slice_files)
        ]
        with Pool(processes=params['num_threads']) as pool:
            results = pool.map(process_chunk, chunk_args)
        if not all(results):
            logger.error("Some chunks failed to process.")
            raise RuntimeError("Parallel processing failed for some chunks.")
        logger.info("Assembling results into final Parquet file...")
        assembler(
            input_dir=os.path.join(basepath, RESULTS_DIR),
            output_path=os.path.join(basepath, OUTPUT_DIR, 'result.parquet')
        )
        logger.info("ETL pipeline completed successfully. Final output: output_dir/result.parquet")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        for var in ['INPUT_PATH', 'OUTPUT_PATH', 'TASK_ID']:
            os.environ.pop(var, None)

if __name__ == '__main__':
    main()