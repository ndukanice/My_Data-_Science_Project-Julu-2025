# US Weather + Energy Analysis Pipeline

This project implements a data pipeline to fetch, process, and analyze historical weather and energy consumption data for several major US cities. The processed data is then visualized through an interactive Streamlit dashboard.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Setup and Installation](#setup-and-installation)
- [Configuration](#configuration)
- [Running the Data Pipeline](#running-the-data-pipeline)
- [Running the Dashboard](#running-the-dashboard)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [License](#license)

## Project Overview

Energy companies can optimize power generation, reduce waste, and lower costs by accurately forecasting demand. This project combines historical weather data (temperature) with energy consumption patterns to provide insights that can aid in better demand forecasting. It demonstrates the ability to build production-ready data pipelines.

## Features

- **Automated Data Pipeline**: Fetches fresh weather (NOAA) and energy (EIA) data daily.
- **Robust Data Fetching**: Includes error handling, logging, and retry mechanisms with exponential backoff for API calls.
- **Data Processing**: Cleans, transforms, and merges weather and energy data.
- **Data Quality Checks**: Identifies missing values, outliers (temperature, negative energy consumption), and flags data freshness.
- **Statistical Analysis**: Performs correlation analysis between temperature and energy consumption.
- **Interactive Dashboard**: A Streamlit application with four key visualizations:
    - **Geographic Overview**: Interactive map showing current temperature and energy usage.
    - **Time Series Analysis**: Dual-axis line chart of temperature and energy consumption over time.
    - **Correlation Analysis**: Scatter plot of temperature vs. energy consumption with regression line.
    - **Usage Patterns Heatmap**: Heatmap showing average energy usage by temperature range and day of week.
- **Production-Ready Code**: Organized Python modules, configurable settings, and comprehensive logging.

## Setup and Installation

To set up the project, follow these steps:

1.  **Clone the repository** (if applicable, otherwise navigate to the project directory):

    ```bash
    git clone <repository_url>
    cd Project_Energy_Analysis
    ```

2.  **Install `uv` (recommended Python package installer)**:

    ```bash
    pip install uv
    ```

3.  **Install dependencies**: Navigate to the project root directory and install the required packages using `uv`:

    ```bash
    uv pip install -r requirements.txt
    # Or if you prefer using pyproject.toml
    # uv pip install .
    ```

    *Note: A `requirements.txt` will be generated from `pyproject.toml` if not present.*

## Configuration

Before running the pipeline, you need to configure your API keys and city details in `config/config.yaml`.

1.  **NOAA Climate Data Online API Key**: 
    - Register for a token at: [https://www.ncdc.noaa.gov/cdo-web/token](https://www.ncdc.noaa.gov/cdo-web/token)
    - Replace `YOUR_TOKEN_HERE` in `config/config.yaml` with your actual NOAA token.

2.  **EIA Open Data API Key**: 
    - Register for an API key at: [https://www.eia.gov/opendata/register.php](https://www.eia.gov/opendata/register.php)
    - Replace `YOUR_API_KEY_HERE` in `config/config.yaml` with your actual EIA API key.

3.  **City and Station IDs**: The `config.yaml` already contains the necessary city, NOAA Station ID, and EIA Region Code mappings. Do not modify these unless you intend to add or change cities.

    ```yaml
    # config/config.yaml
    cities:
      - name: New York
        state: New York
        noaa_station_id: GHCND:USW00094728
        eia_region_code: NYIS
      # ... other cities

    noaa_token: YOUR_TOKEN_HERE
    eia_api_key: YOUR_API_KEY_HERE
    ```

## Running the Data Pipeline

The data pipeline fetches raw data, processes it, performs quality checks, and saves the processed data. It also logs its execution to `logs/pipeline.log`.

To run the pipeline, execute the `pipeline.py` script from the project root:

```bash
python src/pipeline.py
```

This script will:
- Load configuration from `config/config.yaml`.
- Fetch historical weather and energy data for the configured cities.
- Save raw data to `data/raw/`.
- Clean, transform, and merge the raw data.
- Perform data quality checks and log any issues.
- Save the processed data to `data/processed/processed_energy_weather_data.csv`.
- Run basic correlation analysis.

## Running the Dashboard

After successfully running the data pipeline and generating the `processed_energy_weather_data.csv` file, you can launch the Streamlit dashboard.

From the project root directory, run:

```bash
streamlit run dashboards/app.py
```

This will open the dashboard in your web browser, typically at `http://localhost:8501`.

## Project Structure

```
Project_Energy_Analysis/
├── AI_USAGE.md             # AI assistance documentation
├── pyproject.toml          # Project dependencies (using uv)
├── README.md               # Business-focused project summary
├── video_link.md           # Link to your presentation (if applicable)
├── config/
│   └── config.yaml         # API keys, cities list
├── dashboards/
│   └── app.py              # Streamlit application
├── data/
│   ├── processed/          # Clean, analysis-ready data
│   └── raw/                # Original API responses
├── logs/
│   └── pipeline.log        # Execution logs
├── notebooks/
│   └── exploration.ipynb   # Initial analysis (optional)
├── src/
│   ├── analysis.py         # Statistical analysis
│   ├── data_fetcher.py     # API interaction module
│   ├── data_processor.py   # Cleaning and transformation
│   └── pipeline.py         # Main orchestration
└── tests/
    └── test_pipeline.py    # Basic unit tests
```

## Data Sources

- **NOAA Climate Data Online**: Provides historical weather data.
  - API Base URL: `https://www.ncei.noaa.gov/cdo-web/api/v2/data`
- **EIA Open Data**: Provides historical energy consumption data.
  - API Base URL: `https://api.eia.gov/v2/electricity/rto/daily-region-data/data/`

## License

[Specify your project's license here, e.g., MIT, Apache 2.0, etc.]
