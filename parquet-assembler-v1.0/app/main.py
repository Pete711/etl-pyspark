# === BYTENITE ASSEMBLER - MAIN SCRIPT ===

# Read the documentation --> https://docs.bytenite.com/create-with-bytenite/building-blocks/assembling-engines

# == Imports and Environment Variables ==

# Ensure all required external libraries are available in the Docker container image specified in manifest.json under "platform_config" > "container".
try:
    import json
    import os
    import logging
except ImportError as e:
    raise ImportError(f"Required library is missing: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to the folder where the task results from your app executions are stored.
task_results_dir = os.getenv("TASK_RESULTS_DIR")
if task_results_dir is None:
    raise ValueError("Environment variable 'TASK_RESULTS_DIR' is not set.")
if not os.path.isdir(task_results_dir):
    raise ValueError(f"Task result directory '{task_results_dir}' does not exist or is not accessible.")

# Path to the final output directory where your assembler results will be saved. The files in these folder will be uploaded to your data destination.
output_dir = os.getenv("OUTPUT_DIR")
if not output_dir:
    raise ValueError("Environment variable 'OUTPUT_DIR' is not set.")
if not os.path.isdir(output_dir):
    raise ValueError(f"Output directory '{output_dir}' does not exist or is not a directory.")
if not os.access(output_dir, os.W_OK):
    raise ValueError(f"Output directory '{output_dir}' is not writable.")

# The partitioner parameters as imported from the job body in "params" -> "partitioner".
try:
    assembler_params = os.getenv("ASSEMBLER_PARAMS")
    if not assembler_params:
        raise ValueError("Environment variable 'ASSEMBLER_PARAMS' is not set.")
    params = json.loads(assembler_params)
except json.JSONDecodeError:
    raise ValueError("Environment variable 'ASSEMBLER_PARAMS' contains invalid JSON.")


# === Utility Functions ===

def read_result_files():
    """Reads and returns the content of all files in the results directory as a list of strings."""
    result_files = []
    try:
        for filename in os.listdir(task_results_dir):
            file_path = os.path.join(task_results_dir, filename)
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    result_files.append(file.read())
        return result_files
    except OSError as e:
        raise RuntimeError(f"Error accessing source directory '{task_results_dir}': {e}")
    except Exception as e:
        raise RuntimeError(f"Error reading files in '{task_results_dir}': {e}")

def save_output_file(filename, data):
    """Saves the processed output to the specified file in the output directory."""
    output_path = os.path.join(output_dir, filename)
    try:
        with open(output_path, "w") as outfile:
            outfile.write(data)
        logger.info(f"Saved {filename} to {output_path}")
    except OSError as e:
        raise RuntimeError(f"Error writing to output file '{output_path}': {e}")


# === Main Logic ===

if __name__ == "__main__":
    logger.info("Assembler task started")

    # == Your Code ==

    # 1. Reading Result Files
    # Read any files from the results directory. You can use the read_result_files function to read the content of all files in the results directory.


    # ------------------------

    # 2. Fanning In Results
    # If necessary, combine or process the results from multiple files.


    # ------------------------
    # 3. Performing Post-Processing Operations
    # Perform any necessary post-processing operations on the combined results.


    # ------------------------
    # 4. Saving Final Output
    # Save the final output to the output directory using the save_output_file function.


    # -------------------------
