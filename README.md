# Serverless Climate Data Processing Demo

This project demonstrates scalable, serverless processing of climate datasets using Lithops (serverless framework), AWS Lambda, and cloud storage. It is designed to process large climate data (e.g., CMIP6) and derive variables such as Total Precipitable Water (TPW) in a distributed, cost-effective manner.

## Features
- Serverless parallel processing of climate datasets
- Uses Lithops for function orchestration on AWS Lambda
- Example workflow for deriving TPW from Zarr datasets
- Dockerized runtime and reproducible environment
- Example scripts for setup and execution

## Project Structure

| File/Folder                | Description |
|---------------------------|-------------|
| `cmip6_variant_finder.py` | Script to identify CMIP6 model variants with complete data across multiple scenarios |
| `demo_run_lithops.py`      | Main Python script for running Lithops jobs, preprocessing, and deriving variables |
| `pyproject.toml`           | Main dependency management file for use with [uv](https://github.com/astral-sh/uv) |
| `Dockerfile`               | Docker image for custom Lithops runtime |
| `set_environment.sh`       | Setup script to install dependencies and deploy Lithops runtime |
| `.lithops_config_example`  | Example Lithops configuration for AWS Lambda and S3 |
| `urls.csv`                 | List of input dataset URLs |


## Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd serverless-demo
   ```

2. **Install uv (if not already installed)**
   ```bash
   pip install uv
   ```

3. **Install Python dependencies using uv**
   ```bash
   uv sync
   ```
   This will create a virtual environment and install all dependencies as specified in `pyproject.toml`.

4. **Identify CMIP6 Model Variants (Optional)**
   
   The `cmip6_variant_finder.py` script helps identify CMIP6 model variants that have complete data across multiple scenarios for a given variable and time resolution.

   **Basic Usage:**
   ```bash
   # For maximum temperature (default)
   python cmip6_variant_finder.py
   
   # For a different variable (e.g., precipitation)
   python cmip6_variant_finder.py --variable pr
   
   # For daily data
   python cmip6_variant_finder.py --time_resolution day
   
   # For a specific variable and time resolution
   python cmip6_variant_finder.py --variable tas --time_resolution day
   ```

   **Output:**
   The script generates a CSV file named `cmip6_{variable}_{time_resolution}_urls.csv` containing the URLs of the identified model variants.

5. **Edit the input dataset list**
   
   Before running the main processing script, open `urls.csv` and update it to include the S3 URLs for the climate model datasets you want to process. Each row should correspond to a different model or dataset chunk.
   
   If you used the CMIP6 variant finder in the previous step, you can rename its output file to `urls.csv` to use the identified model variants:

   ```bash
   # Example: Using the output from the variant finder
   mv cmip6_tas_Amon_urls.csv urls.csv
   ```

6. **Configure Lithops**
   - Copy `.lithops_config_example` to `.lithops_config` and fill in your AWS credentials and Lambda execution role.

4. **(Optional) Build and Deploy Custom Lithops Runtime**

   If you need additional system or Python packages in your Lithops runtime, edit the `Dockerfile` in this repository. **Whenever you make changes to the `Dockerfile`, you must also change the `IMAGE_TAG` or `IMAGE_NAME` (see below) to give your new image a unique name.** This ensures that the updated image is built, deployed, and used by Lithops, and prevents confusion with previously built images.

   By default, the Docker image will be named `serverless-demo:latest`. You can customize the image name and tag by setting the `IMAGE_NAME` and `IMAGE_TAG` environment variables:

   ```bash
   # Default usage
   ./set_environment.sh

   # Custom image name and tag (recommended after Dockerfile changes)
   IMAGE_NAME=serverless-demo IMAGE_TAG=prod ./set_environment.sh
   ```
   This script installs dependencies, builds the Docker image, and deploys the Lithops runtime to AWS Lambda.

## Usage

**Note:**
- Input Zarr files should be chunked by year to allow efficient processing and to ensure each chunk fits into the memory available for each Lambda invocation.
- If your dataset is too large to fit into Lambda's memory limit, consider further chunking by latitude and longitude as well. This will help prevent memory errors and allow distributed processing of very large datasets.

- **Run the main processing script**
  
  You must specify both the output S3 bucket and the Docker image to use for Lithops. These can be set with the `--output-bucket` and `--image-name` command-line arguments, or by using the `OUTPUT_BUCKET` and `IMAGE_NAME` environment variables. The image name should match the image you built and deployed in the previous setup step, and the output bucket is where results will be written.
  
  ```bash
  IMAGE_NAME=serverless-demo:prod OUTPUT_BUCKET=my-bucket uv run demo_run_lithops.py --mem 3000
  ```
  - `--mem`: Memory (MB) per Lithops worker (default: 3000)
  - `--image-name`: Lithops runtime Docker image name (default: env IMAGE_NAME or 'serverless-demo:latest')
  - `--output-bucket`: Output S3 bucket for results (required if OUTPUT_BUCKET env var is not set)

---

## Project Files

- **pyproject.toml**: Main dependency management file for the project, used by [uv](https://github.com/astral-sh/uv) to install and lock Python dependencies for both local development and Lambda runtime.
- **Dockerfile**: Builds a custom runtime environment for Lithops on AWS Lambda.
- **set_environment.sh**: Automates environment setup and deployment steps.
- **.lithops_config_example**: Template for configuring Lithops with AWS credentials and settings.
- **urls.csv**: CSV containing URLs to input Zarr datasets.


## Contribution Guidelines
- Fork the repository and create a feature branch.
- Add clear docstrings and comments to any new code.
- Open a pull request with a clear description of your changes.
- For substantial changes, please open an issue first to discuss your proposal.



## Acknowledgments
- Built with [Lithops](https://github.com/lithops-cloud/lithops)
- Climate data courtesy of CMIP6 and related projects


## Authors

- James Anheuser <janheuser@cicsnc.org>
