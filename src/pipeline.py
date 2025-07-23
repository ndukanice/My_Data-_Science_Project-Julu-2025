import os
import logging
import data_fetcher, data_processor, analysis

# Configure logging (ensure this matches data_fetcher and data_processor)
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'pipeline.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler()
                    ])

def run_pipeline():
    logging.info("Starting data pipeline execution...")

    # Define paths
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    
    # Ensure raw and processed data directories exist
    os.makedirs(raw_data_path, exist_ok=True)
    os.makedirs(processed_data_path, exist_ok=True)

    # 1. Load Configuration
    config = data_fetcher.load_config(config_path)
    if config is None:
        logging.error("Pipeline aborted: Could not load configuration.")
        return

    # 2. Fetch Raw Data
    logging.info("Fetching raw weather and energy data...")
    weather_data = data_fetcher.fetch_weather_data(config)
    energy_data = data_fetcher.fetch_energy_data(config)

    if weather_data is not None:
        weather_output_path = os.path.join(raw_data_path, 'historical_weather.csv')
        weather_data.to_csv(weather_output_path, index=False)
        logging.info(f"Raw weather data saved to {weather_output_path}")
    else:
        logging.warning("No weather data fetched.")

    if energy_data is not None:
        energy_output_path = os.path.join(raw_data_path, 'historical_energy.csv')
        energy_data.to_csv(energy_output_path, index=False)
        logging.info(f"Raw energy data saved to {energy_output_path}")
    else:
        logging.warning("No energy data fetched.")

    # 3. Process Data
    logging.info("Processing raw data...")
    weather_file = os.path.join(raw_data_path, 'historical_weather.csv')
    energy_file = os.path.join(raw_data_path, 'historical_energy.csv')
    
    # Load raw data for processing (even if fetched in this run, ensure consistency)
    weather_df_proc, energy_df_proc = data_processor.load_raw_data(weather_file, energy_file)
    
    processed_df = data_processor.clean_and_transform_data(weather_df_proc, energy_df_proc)
    quality_report = data_processor.perform_data_quality_checks(processed_df)
    
    processed_output_file = os.path.join(processed_data_path, 'processed_energy_weather_data.csv')
    data_processor.save_processed_data(processed_df, processed_output_file)

    logging.info("--- Data Quality Report Summary ---")
    for key, value in quality_report.items():
        logging.info(f"{key}: {value}")

    # 4. Analyze Data
    logging.info("Performing data analysis...")
    if not processed_df.empty:
        correlation_results = analysis.analyze_correlation(processed_df)
        logging.info("--- Correlation Analysis Results ---")
        for key, value in correlation_results.items():
            logging.info(f"{key}: {value:.2f}")
    else:
        logging.warning("Processed data is empty, skipping analysis.")

    logging.info("Data pipeline execution completed.")

if __name__ == "__main__":
    run_pipeline()
