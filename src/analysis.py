import pandas as pd
import logging
import os
from scipy.stats import pearsonr

# Configure logging
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'pipeline.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler()
                    ])

def load_processed_data(file_path):
    """Loads processed data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        logging.info(f"Loaded processed data from {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"Processed data file not found at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading processed data from {file_path}: {e}")
        return pd.DataFrame()

def analyze_correlation(df):
    """Analyzes the correlation between temperature and energy consumption."""
    logging.info("Performing correlation analysis...")
    correlation_results = {}

    if df.empty or 'max_temp_F' not in df.columns or 'energy_consumption' not in df.columns:
        logging.warning("DataFrame is empty or missing required columns for correlation analysis.")
        return correlation_results

    # Drop rows with NaN values in the relevant columns for correlation calculation
    df_cleaned = df.dropna(subset=['max_temp_F', 'energy_consumption'])

    if df_cleaned.empty:
        logging.warning("No valid data points for correlation analysis after dropping NaNs.")
        return correlation_results

    try:
        correlation, _ = pearsonr(df_cleaned['max_temp_F'], df_cleaned['energy_consumption'])
        correlation_results['overall_correlation'] = correlation
        logging.info(f"Overall correlation between max_temp_F and energy_consumption: {correlation:.2f}")
    except Exception as e:
        logging.error(f"Error calculating overall correlation: {e}")

    # Correlation by city
    for city in df_cleaned['city'].unique():
        city_df = df_cleaned[df_cleaned['city'] == city]
        if len(city_df) > 1: # Need at least 2 data points for correlation
            try:
                correlation, _ = pearsonr(city_df['max_temp_F'], city_df['energy_consumption'])
                correlation_results[f'correlation_{city}'] = correlation
                logging.info(f"Correlation for {city}: {correlation:.2f}")
            except Exception as e:
                logging.error(f"Error calculating correlation for {city}: {e}")
        else:
            logging.warning(f"Not enough data points for correlation analysis in {city}.")

    logging.info("Correlation analysis completed.")
    return correlation_results

if __name__ == "__main__":
    # This allows running the script directly for testing
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed', 'processed_energy_weather_data.csv')
    df = load_processed_data(processed_data_path)
    if not df.empty:
        correlation_results = analyze_correlation(df)
        print("\n--- Correlation Analysis Results ---")
        for key, value in correlation_results.items():
            print(f"{key}: {value:.2f}")
