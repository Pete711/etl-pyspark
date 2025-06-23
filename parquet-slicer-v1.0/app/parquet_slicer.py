import pyarrow.parquet as pq
import pyarrow as pa
import requests
import io
import logging
import platform
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s")
logger = logging.getLogger(__name__)

def check_environment():
    logger.info(f"Python version: {platform.python_version()}")
    logger.info(f"Architecture: {platform.machine()}")
    logger.info(f"OS: {platform.system()} {platform.release()}")
    try:
        import pyarrow
        logger.info(f"PyArrow version: {pyarrow.__version__}")
    except ImportError:
        logger.error("PyArrow is not installed. Install with: pip install pyarrow")
        raise

def partitioner(data_url: str, chunk_size: int, output_dir: str) -> None:
    output_dir = str(Path(output_dir))
    check_environment()
    logger.debug(f"Fetching data from: {data_url}")
    logger.debug(f"Output directory: {output_dir}")
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.debug(f"Created/writable output directory: {output_dir}")
    except OSError as e:
        logger.error(f"Cannot create output directory {output_dir}: {e}")
        raise
    try:
        response = requests.get(data_url, stream=True, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise
    parquet_buffer = io.BytesIO()
    for chunk in response.iter_content(chunk_size=8192):
        parquet_buffer.write(chunk)
    parquet_buffer.seek(0)
    if parquet_buffer.getbuffer().nbytes == 0:
        logger.error("Downloaded Parquet file is empty")
        raise ValueError("Downloaded Parquet file is empty")
    logger.debug("Reading Parquet file in chunks")
    try:
        table = pq.read_table(parquet_buffer)
        total_rows = table.num_rows
        logger.info(f"Total rows: {total_rows}")
        for offset in range(0, total_rows, chunk_size):
            rows_to_read = min(chunk_size, total_rows - offset)
            slice_table = table.slice(offset, rows_to_read)
            slice_path = os.path.join(output_dir, f"slice_{offset // chunk_size}.parquet")
            pq.write_table(slice_table, slice_path, compression="zstd")
            logger.debug(f"Saved slice of {slice_table.num_rows} rows to {slice_path}")
    except Exception as e:
        logger.error(f"Error processing Parquet file: {e}")
        raise

if __name__ == "__main__":
    task_params = {
        "data_url": os.getenv("DATA_URL", "https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet"),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "5000")),
        "output_dir": os.getenv("SLICE_DIR", "../../chunks_dir")
    }
    logger.info(f"Task parameters: {task_params}")
    partitioner(**task_params)