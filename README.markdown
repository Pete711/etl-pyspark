# NYC Taxi ETL Pipeline: Application Flow and Execution Instructions

## Overview

The NYC Taxi ETL Pipeline is a Python-based ETL (Extract, Transform, Load) application that processes NYC taxi trip data (January 2024) from a public Parquet file. It uses PyArrow to partition the dataset into chunks, PySpark to transform each chunk in parallel using multithreading, and pandas/PyArrow to assemble results into a final Parquet file (`output_dir/result.parquet`). The pipeline is designed to run locally or in Docker containers, offering scalability and flexibility for processing large datasets.

This document outlines the ETL pipeline’s flow, recent enhancements, and instructions for running it locally via a Bash script or in Docker using provided configuration files.

## Changes and Additions

The pipeline has been significantly updated from its initial version:

- **Main Script (`etl-pyspark-v1.0/app/main.py`)**:

  - Orchestrates the pipeline, coordinating partitioning, parallel ETL processing, and result assembly.
  - Supports configuration via `params.json` (e.g., `chunk_size`, `dataset_url`, `num_threads`) and `metadata.json` (e.g., `job_name`), with defaults if missing.
  - Uses multithreading (`multiprocessing.Pool`) to process chunks concurrently, improving performance.
  - Manages directories (`source_dir`, `chunks_dir`, `task_results_dir`, `output_dir`) dynamically with robust path handling.
  - Includes enhanced error handling and logging with process-specific identifiers.

- **ETL Script (`etl-pyspark-v1.0/app/nyc_taxi_etl.py`)**:

  - Replaced Pandas with PySpark for distributed processing of chunks, enabling scalability.
  - Implements `extract`, `transform`, and `load` functions to read Parquet chunks, compute metrics (e.g., trip duration, speed, tip percentage), and output text files (`chunk_result_{task_id}.txt`).
  - Configures Spark with low memory settings (1 GB driver/executor) for local execution, optimized for systems like M1 Mac.
  - Outputs results as text via Pandas conversion only at the load stage, minimizing memory usage.

- **Partitioner Script (`parquet-slicer-v1.0/app/parquet_slicer.py`)**:

  - Added to download and split the Parquet dataset into chunks (default: 5,000 rows) using PyArrow.
  - Saves chunks as Parquet files (`slice_{index}.parquet`) in `chunks_dir` with Zstandard compression.
  - Includes environment checks (e.g., Python, PyArrow versions) and detailed logging.

- **Assembler Script (`parquet-assembler-v1.0/app/parquet_assembler.py`)**:

  - Aggregates chunk result text files into a single Parquet file (`result.parquet`) using pandas and PyArrow.
  - Handles empty or invalid files gracefully with warnings.
  - Outputs the final dataset to `output_dir`.

- **Local Execution Script (`test/local.sh`)**:

  - Added a Bash script to run the pipeline locally, injecting environment variables for partitioning, ETL, and assembly.
  - Supports any root directory name by using the current directory as the project root.
  - Clears and creates necessary directories (`source_dir`, `chunks_dir`, `task_results_dir`, `output_dir`).

- **Docker Support**:
  - Provided `Dockerfile` and `docker-compose.yml` to run the pipeline in containers, replacing the previous `keeperofwolves/nyc-taxi-etl:latest` image.
  - Uses `python:3.11-slim` with Spark 3.5.3 and Java 17, optimized for lightweight deployment.
  - Runs three services (`slicer`, `etl`, `assembler`) with shared volumes for data exchange.

These enhancements make the pipeline more modular, scalable, and efficient compared to the initial version, which used Pandas, lacked partitioning, and relied on a single Docker image.

## ETL Application Flow

The pipeline processes NYC taxi trip data in the following steps:

1. **Initialization (`main.py`)**:

   - Sets the project root dynamically based on the current directory or environment variables.
   - Ensures `params.json` and `metadata.json` exist, creating them with defaults if missing:
     ```json
     // params.json
     {
         "chunk_size": 50000,
         "dataset_url": "https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet",
         "num_threads": 4
     }
     // metadata.json
     {
         "job_name": "nyc-taxi-etl",
         "version": "1.0",
         "created_at": "2025-06-20T23:05:00Z"
     }
     ```
   - Creates directories (`source_dir` for temporary chunk files, `chunks_dir` for partitioned files, `task_results_dir` for chunk results, `output_dir` for the final Parquet file).

2. **Partitioning (`parquet_slicer.py`)**:

   - Downloads the Parquet file from the URL in `params.json` or `DATA_URL` environment variable (~100 MB).
   - Uses PyArrow to split the dataset into chunks (default: 5,000 rows).
   - Saves chunks as Parquet files (e.g., `slice_0.parquet`) in `chunks_dir` with Zstandard compression.
   - Logs progress and validates the environment.

3. **Parallel Chunk Processing (`main.py`, `nyc_taxi_etl.py`)**:

   - Iterates over chunk files in `chunks_dir`.
   - Uses `multiprocessing.Pool` to process chunks in parallel (default: 4 threads).
   - For each chunk:
     - Copies the chunk to `source_dir/data_{index}.bin`.
     - Calls `run` from `nyc_taxi_etl.py` to process the chunk.
     - Removes the temporary `data_{index}.bin`.
   - Logs progress (e.g., “Processing slice 1/10”).

4. **ETL Processing (`nyc_taxi_etl.py`)**:

   - **Extract**:
     - Initializes a Spark session with 1 GB memory for driver and executor.
     - Reads the chunk (`data_{index}.bin`) as a Spark DataFrame from Parquet.
   - **Transform**:
     - Drops rows with missing values.
     - Computes metrics:
       - **Trip duration**: (Dropoff time - Pickup time) in minutes.
       - **Speed**: Trip distance ÷ (Trip duration ÷ 60), null if duration is zero.
       - **Airport trip**: True if pickup or dropoff location is an airport (IDs 1, 2, or 3).
       - **Tip percentage**: (Tip amount ÷ Fare amount) × 100, null if fare is zero.
       - **Cost per mile**: Total amount ÷ Trip distance, null if distance is zero.
       - **Pickup hour**: Hour of pickup time.
       - **Peak hour**: True if pickup hour is 7–9 AM or 5–7 PM.
       - **Trip summary**: Concatenated string (e.g., “Trip from location 123 to 456 with 2 passenger(s), covering 5.2 miles in 15.3 minutes.”).
   - **Load**:
     - Converts the Spark DataFrame to Pandas for text output.
     - Saves results to `task_results_dir/chunk_result_{task_id}.txt` with headers and separators.

5. **Assembly (`parquet_assembler.py`)**:

   - Reads all `chunk_result_{task_id}.txt` files from `task_results_dir`.
   - Combines them into a single pandas DataFrame, skipping empty or invalid files.
   - Saves the final dataset as `output_dir/result.parquet` using PyArrow with Zstandard compression.
   - Logs the number of files combined and rows in the output.

6. **Completion**:
   - `main.py` logs pipeline completion.
   - The final `result.parquet` is accessible in `output_dir/`.

## Prerequisites

### Local Execution

- **Python**: 3.8+ (tested with 3.9.13).
- **Java**: 8 or 11 (required for PySpark).
- **Dependencies**: Install via `requirements.txt`:
  ```bash
  pip install -r requirements.txt
  ```
  Contents of `requirements.txt`:
  ```
  pandas>=2.0.0
  pyarrow>=12.0.0
  requests>=2.28.0
  pyspark>=3.5.3
  ```
- **Disk Space**: ~1 GB for dataset, chunks, and output.
- **Internet Access**: To download the dataset.

Verify Python and Java:

```bash
python3 --version
java -version
```

### Docker Execution

- **Docker**: Installed (Docker Desktop or CLI). Download from [docker.com](https://www.docker.com/products/docker-desktop/).
- **Disk Space**: ~5 GB for the container, Spark, and output files.
- **Internet Access**: To build the image and download the dataset.

Verify Docker:

```bash
docker --version
```

## Local Execution Instructions

Run the pipeline locally using the provided Bash script (`test/local.sh`), which sets environment variables and executes the components sequentially.

1. **Navigate to the Project Root**:

   ```bash
   cd /path/to/nyc-taxi-etl
   ```

2. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Ensure Directory Structure**:
   Create required directories if missing:

   ```bash
   mkdir -p etl-pyspark-v1.0/app parquet-assembler-v1.0/app parquet-slicer-v1.0/app test source_dir chunks_dir task_results_dir output_dir
   ```

4. **Verify Scripts**:
   Ensure the following files are in place:

   - `etl-pyspark-v1.0/app/main.py`
   - `etl-pyspark-v1.0/app/nyc_taxi_etl.py`
   - `parquet-assembler-v1.0/app/parquet_assembler.py`
   - `parquet-slicer-v1.0/app/parquet_slicer.py`
   - `test/local.sh`

5. **Create `params.json` (Optional)**:
   Place in the project root to customize settings:

   ```json
   {
     "chunk_size": 5000,
     "dataset_url": "https://storage.googleapis.com/video-test-public/yellow_tripdata_2024-01.parquet",
     "num_threads": 4
   }
   ```

6. **Run the Script**:

   ```bash
   cd test
   chmod +x local.sh
   ./local.sh
   ```

7. **Verify Output**:
   - Check logs for progress (e.g., “Running parquet slicer...”).
   - Verify `output_dir/result.parquet`:
     ```python
     import pandas as pd
     df = pd.read_parquet("output_dir/result.parquet")
     print(df.head())
     ```

## Docker Execution Instructions

Build and run the pipeline using the provided `Dockerfile` and `docker-compose.yml`, which define three services (`slicer`, `etl`, `assembler`) with shared volumes.

1. **Navigate to the Project Root**:

   ```bash
   cd /path/to/nyc-taxi-etl
   ```

2. **Ensure Directory Structure**:
   Create directories for volumes:

   ```bash
   mkdir -p source_dir chunks_dir task_results_dir output_dir
   ```

3. **Create `params.json` (Optional)**:
   As above, place in the project root.

4. **Build the Docker Image**:

   ```bash
   docker build -t etl-pyspark .
   ```

5. **Run with Docker Compose**:

   ```bash
   docker-compose up --build
   ```

   This runs the services sequentially:

   - `slicer`: Partitions the dataset.
   - `etl`: Processes chunks in parallel.
   - `assembler`: Combines results.

6. **Verify Output**:

   - Check container logs:
     ```bash
     docker-compose logs
     ```
   - Verify `output_dir/result.parquet`:
     ```python
     import pandas as pd
     df = pd.read_parquet("output_dir/result.parquet")
     print(df.head())
     ```

7. **Clean Up**:
   Stop and remove containers:
   ```bash
   docker-compose down
   ```

## Troubleshooting

### Local Execution

- **Script Not Found**:
  - Verify script paths:
    ```bash
    ls -R .
    ```
  - Ensure `parquet_slicer.py`, `main.py`, `nyc_taxi_etl.py`, `parquet_assembler.py` are in their respective `app/` subdirectories.
- **Import Errors**:
  - Check `PYTHONPATH` in `local.sh` includes `etl-pyspark-v1.0/app/`, `parquet-slicer-v1.0/app/`, `parquet-assembler-v1.0/app/`.
- **Dependency Issues**:
  - Reinstall:
    ```bash
    pip install -r requirements.txt
    ```
  - Ensure Java is installed:
    ```bash
    java -version
    ```
- **No Output**:
  - Check logs in the terminal.
  - Verify `output_dir/` is writable:
    ```bash
    chmod -R u+w output_dir/
    ```

### Docker Execution

- **Build Fails**:
  - Verify `Dockerfile`, `docker-compose.yml`, and `requirements.txt` are in the project root.
  - Check internet connectivity for downloading Spark and dependencies.
- **Run Fails**:
  - View logs:
    ```bash
    docker-compose logs etl
    ```
  - Check for:
    - Network issues downloading the dataset.
    - Insufficient memory (adjust Docker resource limits).
    - Invalid `params.json` (validate JSON syntax).
- **No Output**:
  - Ensure volume mounts (`-v` in `docker-compose.yml`) point to existing host directories.
  - Verify containers completed successfully.
- **Performance**:
  - Adjust `num_threads` or `chunk_size` in `params.json` (smaller `chunk_size` for low memory, fewer `num_threads` for fewer cores).
  - Ensure sufficient disk space for chunks and results.

## Directory Structure

```
./
├── etl-pyspark-v1.0/
│   ├── app/
│   │   ├── main.py
│   │   ├── nyc_taxi_etl.py
│   │   └── manifest.json
├── parquet-assembler-v1.0/
│   ├── app/
│   │   ├── parquet_assembler.py
│   │   └── manifest.json
├── parquet-slicer-v1.0/
│   ├── app/
│   │   ├── parquet_slicer.py
│   │   └── manifest.json
├── test/
│   └── local.sh
├── source_dir/
├── chunks_dir/
├── task_results_dir/
├── output_dir/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── template.json
```

## Notes

- The pipeline is optimized for local execution on systems like M1 Mac and containerized deployment.
- The default `chunk_size` (5,000) and `num_threads` (4) balance memory and performance; adjust for your system.
- The final output (`result.parquet`) is a Parquet file, suitable for further analysis with pandas or PyArrow.
- For ByteNite-specific configurations (e.g., `template.json`), consult ByteNite documentation or provide details for customization.

For issues or enhancements, please provide logs, directory listings, or specific requirements.
