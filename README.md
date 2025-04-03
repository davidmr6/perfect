# Perfect dbt Project

## Prerequisites
- Python 3.11
- Google Cloud Platform account with BigQuery access
- Google Cloud SDK installed

## Installation

1. Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # For macOS/Linux
    ```

pip install  BigQuery Setup

1. Configure BigQuery Authentication:
   - Set up a service account in GCP with BigQuery access
   - Download the JSON key file
   - Set the environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

## Setup

1. Configure BigQuery Authentication:
   - Set up a service account in GCP with BigQuery access
   - Download the JSON key file
   - Set the environment variable:
   . Create BigQuery Dataset:
   - Go to BigQuery Console
   - Create a new dataset named `raw_data`
   - Upload `events.csv` to the `raw_data` dataset

## Project Configuration

1. Configure dbt Profile:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
   ```
