import pandas as pd
import logging
import os

# Configure logging
log_file_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'pipeline.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler()
                    ])

def load_raw_data(weather_path, energy_path):
    """Loads raw weather and energy data from CSV files."""
    try:
        weather_df = pd.read_csv(weather_path)
        logging.info(f"Loaded weather data from {weather_path}")
    except FileNotFoundError:
        logging.error(f"Weather data file not found at {weather_path}")
        weather_df = pd.DataFrame()

    try:
        energy_df = pd.read_csv(energy_path)
        logging.info(f"Loaded energy data from {energy_path}")
    except FileNotFoundError:
        logging.error(f"Energy data file not found at {energy_path}")
        energy_df = pd.DataFrame()

    return weather_df, energy_df

def clean_and_transform_data(weather_df, energy_df):
    """Cleans and transforms weather and energy data."""
    logging.info("Starting data cleaning and transformation...")

    # --- Weather Data Cleaning and Transformation ---
    if not weather_df.empty:
        weather_df['date'] = pd.to_datetime(weather_df['date'])
        # Pivot weather data
        weather_df = weather_df.pivot_table(index=['date', 'city'], columns='datatype', values='value').reset_index()
        weather_df.rename(columns={'TMAX': 'max_temp_F', 'TMIN': 'min_temp_F'}, inplace=True)
        # Ensure essential columns are kept
        weather_df = weather_df[['date', 'city', 'max_temp_F', 'min_temp_F']]
        logging.info("Weather data cleaned and transformed.")
    else:
        logging.warning("Weather DataFrame is empty, skipping cleaning and transformation.")

    # --- Energy Data Cleaning and Transformation ---
    if not energy_df.empty:
        energy_df.rename(columns={'period': 'date', 'value': 'energy_consumption', 'region': 'city'}, inplace=True)
        energy_df['date'] = pd.to_datetime(energy_df['date'])
        energy_df = energy_df[['date', 'city', 'energy_consumption']]
        logging.info("Energy data cleaned and transformed.")
    else:
        logging.warning("Energy DataFrame is empty, skipping cleaning and transformation.")
    # --- Merge Data ---
    if not weather_df.empty and not energy_df.empty:
        merged_df = pd.merge(weather_df, energy_df, on=['date', 'city'], how='inner')
        logging.info("Weather and energy data merged.")
        if not merged_df.empty:
            # Calculate daily energy usage change
            merged_df.drop_duplicates(inplace=True)
            merged_df = merged_df[merged_df['energy_consumption'] >= 0]
            merged_df['energy_change'] = merged_df.groupby('city')['energy_consumption'].diff()
            
            # Add features
            merged_df['day_of_week'] = merged_df['date'].dt.day_name()
            merged_df['is_weekend'] = merged_df['date'].dt.dayofweek >= 5 # Saturday=5, Sunday=6
            logging.info("Additional features added to merged data.")
    elif not weather_df.empty:
        merged_df = weather_df
        logging.warning("Energy data is empty, merged DataFrame contains only weather data.")
    elif not energy_df.empty:
        merged_df = energy_df
        logging.warning("Weather data is empty, merged DataFrame contains only energy data.")
    else:
        merged_df = pd.DataFrame()
        logging.warning("Both weather and energy DataFrames are empty, merged DataFrame is empty.")

    return merged_df

def perform_data_quality_checks(df):
    """Performs data quality checks and logs issues."""
    logging.info("Performing data quality checks...")
    quality_report = {}

    if df.empty:
        logging.warning("DataFrame is empty, skipping data quality checks.")
        return quality_report

    # Duplicates
    duplicates = df[df.duplicated()]
    if not duplicates.empty:
        logging.warning(f"Duplicate records found:\n{duplicates}")
        quality_report['duplicates'] = duplicates.to_dict(orient='records')
    else:
        logging.info("No duplicate records found.")
        quality_report['duplicates'] = "None"

    # Missing Values
    missing_values = df.isnull().sum()
    missing_values = missing_values[missing_values > 0]
    if not missing_values.empty:
        logging.warning(f"Missing values found:\n{missing_values}")
        quality_report['missing_values'] = missing_values.to_dict()
    else:
        logging.info("No missing values found.")
        quality_report['missing_values'] = "None"

    # Outliers (Temperature)
    temp_outliers_high = df[df['max_temp_F'] > 130]
    temp_outliers_low = df[df['min_temp_F'] < -50]
    if not temp_outliers_high.empty:
        logging.warning(f"High temperature outliers found (max_temp_F > 130F):\n{temp_outliers_high}")
        quality_report['high_temp_outliers'] = temp_outliers_high.to_dict(orient='records')
    if not temp_outliers_low.empty:
        logging.warning(f"Low temperature outliers found (min_temp_F < -50F):\n{temp_outliers_low}")
        quality_report['low_temp_outliers'] = temp_outliers_low.to_dict(orient='records')
    if temp_outliers_high.empty and temp_outliers_low.empty:
        logging.info("No temperature outliers found.")
        quality_report['temp_outliers'] = "None"

    # Outliers (Negative Energy Consumption)
    negative_energy = df[df['energy_consumption'] < 0]
    if not negative_energy.empty:
        logging.warning(f"Negative energy consumption values found:\n{negative_energy}")
        quality_report['negative_energy_consumption'] = negative_energy.to_dict(orient='records')
    else:
        logging.info("No negative energy consumption values found.")
        quality_report['negative_energy_consumption'] = "None"

    # Data Freshness (assuming data should be recent, e.g., within last 2 days)
    if 'date' in df.columns and not df.empty:
        latest_date = df['date'].max()
        from datetime import date as dt_date
        if (dt_date.today() - latest_date.date()).days > 2:
            logging.warning(f"Data might be stale. Latest date in data: {latest_date.date()}")
            quality_report['data_freshness'] = f"Stale (latest date: {latest_date.date()})"
        else:
            logging.info("Data is fresh.")
            quality_report['data_freshness'] = "Fresh"
    else:
        logging.warning("Cannot check data freshness: 'date' column missing or DataFrame is empty.")
        quality_report['data_freshness'] = "Unknown (missing date column or empty DataFrame)"

    logging.info("Data quality checks completed.")
    return quality_report

def save_processed_data(df, output_path):
    """Saves the processed DataFrame to a CSV file."""
    if not df.empty:
        try:
            df.to_csv(output_path, index=False)
            logging.info(f"Processed data saved to {output_path}")
        except Exception as e:
            logging.error(f"Error saving processed data to {output_path}: {e}")
    else:
        logging.warning("Processed DataFrame is empty, not saving.")

if __name__ == "__main__":
    # This allows running the script directly for testing
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    
    weather_file = os.path.join(raw_data_path, 'historical_weather.csv')
    energy_file = os.path.join(raw_data_path, 'historical_energy.csv')
    output_file = os.path.join(processed_data_path, 'processed_energy_weather_data.csv')

    weather_df, energy_df = load_raw_data(weather_file, energy_file)
    processed_df = clean_and_transform_data(weather_df, energy_df)
    quality_report = perform_data_quality_checks(processed_df)
    save_processed_data(processed_df, output_file)

    logging.info("--- Data Quality Report ---")
    for key, value in quality_report.items():
        logging.info(f"{key}: {value}")
