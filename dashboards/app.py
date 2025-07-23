import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
import sys

# Add src directory to path to allow imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data_processor import perform_data_quality_checks

# Set page config
st.set_page_config(layout="wide", page_title="US Weather + Energy Analysis")

# --- Data Loading ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_data():
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed', 'processed_energy_weather_data.csv')
    try:
        df = pd.read_csv(processed_data_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except FileNotFoundError:
        st.error("Processed data file not found. Please run the data pipeline first.")
        return pd.DataFrame()

df = load_data()

# --- Sidebar Filters ---
st.sidebar.header("Filters")
st.sidebar.markdown("Built by: **Emmanuel Eze**")
city_filter = st.sidebar.multiselect("Select Cities", options=sorted(df['city'].unique()), default=sorted(df['city'].unique()))
start_date = st.sidebar.date_input("Start Date", value=df['date'].min() if not df.empty else None)
end_date = st.sidebar.date_input("End Date", value=df['date'].max() if not df.empty else None)

# --- Filter Data ---
df_filtered = df.copy()
if not df.empty:
    if city_filter:
        df_filtered = df_filtered[df_filtered['city'].isin(city_filter)]
    if start_date:
        df_filtered = df_filtered[df_filtered['date'] >= pd.to_datetime(start_date)]
    if end_date:
        df_filtered = df_filtered[df_filtered['date'] <= pd.to_datetime(end_date)]

# --- Main Dashboard ---

# --- Data Quality Report ---
with st.expander("View Data Quality Report"):
    st.header("Data Quality Report")
    if not df.empty:
        quality_report = perform_data_quality_checks(df.copy()) 
        
        st.subheader("Data Freshness")
        freshness_status = quality_report.get('data_freshness', 'Unknown')
        if "Stale" in freshness_status:
            st.warning(f"**Status:** {freshness_status}")
        else:
            st.success(f"**Status:** {freshness_status}")

        st.subheader("Missing Values")
        missing_values = quality_report.get('missing_values')
        if isinstance(missing_values, dict) and any(missing_values.values()):
            st.warning("Missing values found.")
            st.dataframe(pd.DataFrame.from_dict(missing_values, orient='index', columns=['Count']))
        else:
            st.success("No missing values found.")

        st.subheader("Outliers")
        high_temp_outliers = quality_report.get('high_temp_outliers')
        low_temp_outliers = quality_report.get('low_temp_outliers')
        neg_energy_outliers = quality_report.get('negative_energy_consumption')

        if high_temp_outliers and isinstance(high_temp_outliers, list):
            st.warning("High temperature outliers detected ( > 130°F).")
            st.dataframe(pd.DataFrame(high_temp_outliers))
        else:
            st.success("No high temperature outliers found.")
        
        if low_temp_outliers and isinstance(low_temp_outliers, list):
            st.warning("Low temperature outliers detected ( < -50°F).")
            st.dataframe(pd.DataFrame(low_temp_outliers))
        else:
            st.success("No low temperature outliers found.")

        if neg_energy_outliers and isinstance(neg_energy_outliers, list):
            st.warning("Negative energy consumption values detected.")
            st.dataframe(pd.DataFrame(neg_energy_outliers))
        else:
            st.success("No negative energy consumption values found.")
    else:
        st.info("No data available to generate a quality report.")

st.title("US Weather + Energy Analysis Pipeline Dashboard")

if not df_filtered.empty:
    last_updated = df_filtered['date'].max().strftime("%Y-%m-%d %H:%M:%S")
    st.info(f"Data last updated: {last_updated}")

    # --- Visualization 1: Geographic Overview ---
    st.header("1. Geographic Overview")
    st.write("Interactive map showing current temperature and energy usage for selected cities.")

    # Get latest data for each city
    latest_data = df_filtered.loc[df_filtered.groupby('city')['date'].idxmax()]
    
    # Dummy coordinates for cities
    city_coords = {
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437},
    "Chicago": {"lat": 41.8781, "lon": -87.6298},
    "Houston": {"lat": 29.7604, "lon": -95.3698},
    "Phoenix": {"lat": 33.4484, "lon": -112.0740},
    "Philadelphia": {"lat": 39.9526, "lon": -75.1652},
    "San Antonio": {"lat": 29.4241, "lon": -98.4936},
    "San Diego": {"lat": 32.7157, "lon": -117.1611},
    "Dallas": {"lat": 32.7767, "lon": -96.7970},
    "San Jose": {"lat": 37.3382, "lon": -121.8863},
    "Austin": {"lat": 30.2672, "lon": -97.7431},
    "Jacksonville": {"lat": 30.3322, "lon": -81.6557},
    "Fort Worth": {"lat": 32.7555, "lon": -97.3308},
    "Columbus": {"lat": 39.9612, "lon": -82.9988},
    "Charlotte": {"lat": 35.2271, "lon": -80.8431},
    "San Francisco": {"lat": 37.7749, "lon": -122.4194},
    "Indianapolis": {"lat": 39.7684, "lon": -86.1581},
    "Seattle": {"lat": 47.6062, "lon": -122.3321},
    "Denver": {"lat": 39.7392, "lon": -104.9903},
    "Washington": {"lat": 38.9072, "lon": -77.0369},
    "Boston": {"lat": 42.3601, "lon": -71.0589},
    "El Paso": {"lat": 31.7619, "lon": -106.4850},
    "Nashville": {"lat": 36.1627, "lon": -86.7816},
    "Detroit": {"lat": 42.3314, "lon": -83.0458},
    "Oklahoma City": {"lat": 35.4676, "lon": -97.5164},
    "Portland": {"lat": 45.5152, "lon": -122.6784},
    "Las Vegas": {"lat": 36.1699, "lon": -115.1398},
    "Memphis": {"lat": 35.1495, "lon": -90.0490},
    "Louisville": {"lat": 38.2527, "lon": -85.7585},
    "Baltimore": {"lat": 39.2904, "lon": -76.6122},
    "Milwaukee": {"lat": 43.0389, "lon": -87.9065},
    "Albuquerque": {"lat": 35.0844, "lon": -106.6504},
    "Tucson": {"lat": 32.2226, "lon": -110.9747},
    "Fresno": {"lat": 36.7468, "lon": -119.7726},
    "Sacramento": {"lat": 38.5816, "lon": -121.4944},
    "Kansas City": {"lat": 39.0997, "lon": -94.5786},
    "Mesa": {"lat": 33.4152, "lon": -111.8315},
    "Atlanta": {"lat": 33.7490, "lon": -84.3880},
    "Omaha": {"lat": 41.2565, "lon": -95.9345},
    "Colorado Springs": {"lat": 38.8339, "lon": -104.8214},
    "Raleigh": {"lat": 35.7796, "lon": -78.6382},
    "Miami": {"lat": 25.7617, "lon": -80.1918},
    "Long Beach": {"lat": 33.7701, "lon": -118.1937},
    "Virginia Beach": {"lat": 36.8529, "lon": -75.9780},
    "Oakland": {"lat": 37.8044, "lon": -122.2712},
    "Minneapolis": {"lat": 44.9778, "lon": -93.2650},
    "Tulsa": {"lat": 36.1540, "lon": -95.9928},
    "Arlington": {"lat": 32.7357, "lon": -97.1081},
    "Tampa": {"lat": 27.9506, "lon": -82.4572},
    "New Orleans": {"lat": 29.9511, "lon": -90.0715}
}
    
    map_data = latest_data.copy()
    map_data['lat'] = map_data['city'].map(lambda x: city_coords.get(x, {}).get('lat'))
    map_data['lon'] = map_data['city'].map(lambda x: city_coords.get(x, {}).get('lon'))
    map_data.dropna(subset=['lat', 'lon', 'energy_consumption'], inplace=True)

    if not map_data.empty:
        valid_energy = map_data['energy_consumption'].dropna().unique()
        if len(valid_energy) > 1:
            map_data['energy_color'] = pd.qcut(map_data['energy_consumption'], q=2, labels=['green', 'red'])
        else:
            map_data['energy_color'] = 'green'

        fig_map = px.scatter_mapbox(
            map_data,
            lat="lat",
            lon="lon",
            color="energy_color",
            size="energy_consumption",
            hover_name="city",
            hover_data={
                "max_temp_F": True,
                "min_temp_F": True,
                "energy_consumption": True,
                "energy_color": False,
            },
            color_discrete_map={'green': 'green', 'red': 'red'},
            zoom=3,
            height=500
        )
        fig_map.update_layout(mapbox_style="open-street-map")
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("No data with valid coordinates for Geographic Overview.")

    # --- Visualization 2: Time Series Analysis ---
    st.header("2. Time Series Analysis")
    st.write("Dual-axis line chart showing temperature and energy consumption over time.")

    time_series_city = st.selectbox(
        "Select City for Time Series",
        options=sorted(df_filtered['city'].unique().tolist() + ["All Cities"]),
        index=0
    )

    if time_series_city != "All Cities":
        ts_df = df_filtered[df_filtered['city'] == time_series_city].sort_values('date')
    else:
        ts_df = df_filtered.groupby('date')[['max_temp_F', 'min_temp_F', 'energy_consumption']].mean().reset_index().sort_values('date')
        ts_df['city'] = "All Cities"

    if not ts_df.empty:
        fig_ts = go.Figure()

        fig_ts.add_trace(go.Scatter(
            x=ts_df['date'],
            y=ts_df['max_temp_F'],
            mode='lines',
            name='Max Temp (F)',
            line=dict(color='red'),
            yaxis='y1'
        ))
        fig_ts.add_trace(go.Scatter(
            x=ts_df['date'],
            y=ts_df['min_temp_F'],
            mode='lines',
            name='Min Temp (F)',
            line=dict(color='orange'),
            yaxis='y1'
        ))

        fig_ts.add_trace(go.Scatter(
            x=ts_df['date'],
            y=ts_df['energy_consumption'],
            mode='lines',
            name='Energy Consumption',
            line=dict(color='blue', dash='dot'),
            yaxis='y2'
        ))

        weekends = ts_df[ts_df['date'].dt.dayofweek >= 5]
        for i in range(len(weekends) - 1):
            if weekends.iloc[i]['date'].date() != weekends.iloc[i+1]['date'].date():
                fig_ts.add_vrect(x0=weekends.iloc[i]['date'], x1=weekends.iloc[i]['date'] + timedelta(days=1),
                                fillcolor="LightSalmon", opacity=0.2, layer="below", line_width=0)

        fig_ts.update_layout(
            title=f'Temperature and Energy Consumption Over Time ({time_series_city})',
            xaxis_title='Date',
            yaxis=dict(
                title='Temperature (F)',
                tickfont=dict(color='red')
            ),
            yaxis2=dict(
                title='Energy Consumption',
                tickfont=dict(color='blue'),
                overlaying='y',
                side='right'
            ),
            hovermode="x unified"
        )
        st.plotly_chart(fig_ts, use_container_width=True)
    else:
        st.info("No data available for the selected city and date range.")

       # --- Visualization 3: Correlation Analysis ---
    st.header("3. Correlation Analysis")
    st.write("Scatter plot of temperature vs. energy consumption with regression line.")

    corr_df = df_filtered.dropna(subset=['max_temp_F', 'energy_consumption'])
    if not corr_df.empty:
        fig_corr = px.scatter(
            corr_df,
            x="max_temp_F",
            y="energy_consumption",
            color="city",
            title="Temperature vs. Energy Consumption",
            labels={'max_temp_F': 'Max Temperature (F)', 'energy_consumption': 'Energy Consumption'},
            trendline="ols",
            hover_data={'date': True, 'city': True, 'max_temp_F': ':.2f', 'energy_consumption': ':.2f'}
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("No complete data points for correlation analysis after filtering.")

    # --- Visualization 4: Usage Patterns Heatmap ---
    st.header("4. Usage Patterns Heatmap")
    st.write("Heatmap showing average energy usage by temperature range and day of week.")
    
    if 'day_of_week' in df_filtered.columns and 'max_temp_F' in df_filtered.columns:
        bins = [-float('inf'), 50, 60, 70, 80, 90, float('inf')]
        labels = ['<50°F', '50-60°F', '60-70°F', '70-80°F', '80-90°F', '>90°F']
        df_filtered['temp_range'] = pd.cut(df_filtered['max_temp_F'], bins=bins, labels=labels, right=False)

        heatmap_data = df_filtered.groupby(['temp_range', 'day_of_week'])['energy_consumption'].mean().unstack()
        
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Ensure all days are present in the columns, fill missing with 0 or NaN
        for day in day_order:
            if day not in heatmap_data.columns:
                heatmap_data[day] = 0 # Or use np.nan if you prefer
        
        heatmap_data = heatmap_data[day_order]

        if not heatmap_data.empty:
            fig_heatmap = px.imshow(
                heatmap_data,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                color_continuous_scale=['blue', 'red'],
                title="Average Energy Consumption by Temperature Range and Day of Week",
                labels={'x': 'Day of Week', 'y': 'Temperature Range', 'color': 'Avg. Energy Consumption'},
                text_auto=True
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("No data available to generate heatmap after filtering.")
    else:
        st.warning("Required columns for heatmap are missing.")
else:
    st.warning("No data loaded or available after initial filtering. Please ensure the pipeline has run and data exists.")