import yaml
import requests
import pandas as pd
from datetime import date, timedelta
import logging
import os
import time
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError

# Configure logging
log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs', 'pipeline.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def load_config(config_path):
    """Loads the configuration file."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"Error: Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file {config_path}: {e}")
        return None

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def _fetch_url_with_retry(url, headers=None, params=None):
    """Helper function to fetch URL with retry logic."""
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response

def fetch_weather_data(config):
    """
    Fetches historical weather data (TMAX, TMIN) for the last 90 days for all cities specified in the config file.
    Returns a pandas DataFrame.
    """
    noaa_token = config.get('api_keys', {}).get('noaa')
    if not noaa_token or noaa_token == 'YOUR_TOKEN_HERE':
        logging.error("Error: NOAA token not found or not set in config.yaml.")
        return None

    headers = {'token': noaa_token}
    base_url = config.get('api_urls', {}).get('noaa', "https://www.ncei.noaa.gov/cdo-web/api/v2/data")
    end_date = date.today()
    start_date = end_date - timedelta(days=90)

    all_weather_data = []
    for city in config['cities']:
        logging.info(f"Fetching weather data for {city['name']}...")
        params = {
            'datasetid': 'GHCND',
            'datatypeid': 'TMAX,TMIN',
            'units': 'standard', # Use standard units (Fahrenheit)
            'startdate': start_date.strftime('%Y-%m-%d'),
            'enddate': end_date.strftime('%Y-%m-%d'),
            'stationid': city['noaa_station_id'],
            'limit': 1000
        }
        try:
            response = _fetch_url_with_retry(base_url, headers=headers, params=params)
            data = response.json()
            if 'results' in data:
                df = pd.DataFrame(data['results'])
                df['city'] = city['name']
                all_weather_data.append(df)
                logging.info(f"Successfully fetched {len(df)} records for {city['name']}.")
            else:
                logging.warning(f"No results found for {city['name']}.")
        except RetryError as e:
            logging.error(f"Failed to fetch data for {city['name']} after multiple retries: {e}")
            return None # Exit if any city fails
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {city['name']}: {e}")
            return None # Exit if any city fails
        time.sleep(0.5) # Adjusted sleep time

    if not all_weather_data:
        logging.warning("No weather data was fetched for any city.")
        return None

    final_df = pd.concat(all_weather_data, ignore_index=True)
    return final_df

def fetch_energy_data(config):
    """
    Fetches historical energy consumption data for the last 90 days for all regions specified in the config file.
    Returns a pandas DataFrame.
    """
    eia_api_key = config.get('api_keys', {}).get('eia')
    if not eia_api_key or eia_api_key == 'YOUR_API_KEY_HERE':
        logging.error("Error: EIA API key not found or not set in config.yaml.")
        return None

    base_url = config.get('api_urls', {}).get('eia', "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/")
    end_date = date.today()
    start_date = end_date - timedelta(days=90)

    all_energy_data = []
    for city in config['cities']:
        logging.info(f"Fetching energy data for {city['eia_region_code']}...")
        params = {
            'api_key': eia_api_key,
            'frequency': 'daily',
            'data[0]': 'value',
            'facets[respondent][]': city['eia_region_code'],
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d'),
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc'
        }
        try:
            response = _fetch_url_with_retry(base_url, params=params)
            data = response.json()
            if 'response' in data and 'data' in data['response'] and data['response']['data']:
                df = pd.DataFrame(data['response']['data'])
                df['region'] = city['name'] # Use city name for consistency
                all_energy_data.append(df)
                logging.info(f"Successfully fetched {len(df)} records for {city['eia_region_code']}.")
            else:
                logging.warning(f"No results found for {city['eia_region_code']}. Response: {data}")
        except RetryError as e:
            logging.error(f"Failed to fetch data for {city['eia_region_code']} after multiple retries: {e}")
            return None # Exit if any city fails
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {city['eia_region_code']}: {e}")
            return None # Exit if any city fails
        time.sleep(0.5) # Adjusted sleep time

    if not all_energy_data:
        logging.warning("No energy data was fetched for any region.")
        return None

    final_df = pd.concat(all_energy_data, ignore_index=True)
    return final_df

if __name__ == "__main__":
    # Use absolute path for config file
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml'))
    config = load_config(config_path)
    if config:
        # Fetch Weather Data
        weather_data = fetch_weather_data(config)
        if weather_data is not None:
            logging.info("\n--- Fetched Weather Data ---")
            logging.info(weather_data.head())
            output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'historical_weather.csv'))
            weather_data.to_csv(output_path, index=False)
            logging.info(f"\nWeather data saved to {output_path}")

        # Fetch Energy Data
        energy_data = fetch_energy_data(config)
        if energy_data is not None:
            logging.info("\n--- Fetched Energy Data ---")
            logging.info(energy_data.head())
            output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'historical_energy.csv'))
            energy_data.to_csv(output_path, index=False)
            logging.info(f"\nEnergy data saved to {output_path}")
