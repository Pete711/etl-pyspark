import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s")
logger = logging.getLogger(__name__)

def assembler(input_dir: str, output_path: str) -> None:
    input_dir = str(Path(input_dir))
    output_path = str(Path(output_path))
    logger.debug(f"Merging results from {input_dir}")
    try:
        result_dfs = []
        for filename in os.listdir(input_dir):
            if filename.startswith("chunk_result_") and filename.endswith(".txt"):
                file_path = os.path.join(input_dir, filename)
                try:
                    df = pd.read_csv(file_path, skiprows=2, skipfooter=1, engine='python')
                    if not df.empty:
                        result_dfs.append(df)
                        logger.debug(f"Read {file_path} with {len(df)} rows")
                    else:
                        logger.warning(f"Empty dataframe from {file_path}, skipping")
                except Exception as e:
                    logger.warning(f"Failed to read {file_path}: {e}, skipping")
        if not result_dfs:
            logger.error("No valid result files found")
            raise ValueError("No valid result files to assemble")
        combined_df = pd.concat(result_dfs, ignore_index=True)
        logger.info(f"Combined {len(result_dfs)} result files with {len(combined_df)} rows")
        table = pa.Table.from_pandas(combined_df)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pq.write_table(table, output_path, compression="zstd")
        logger.info(f"Saved Parquet file to {output_path}")
    except Exception as e:
        logger.error(f"Error assembling results: {e}")
        raise

if __name__ == "__main__":
    task_params = {
        "input_dir": os.getenv("RESULTS_DIR", "../../task_results_dir"),
        "output_path": os.getenv("OUTPUT_PATH", "../../output_dir/result.parquet")
    }
    assembler(**task_params)