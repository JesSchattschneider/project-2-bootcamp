import streamlit as st
import snowflake.connector
import os
from dotenv import load_dotenv
import datetime
import pandas as pd
import plotly.graph_objects as go

load_dotenv()

# Snowflake connection parameters
snowflake_user = os.environ.get('SNOWFLAKE_USER')
snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')
snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
snowflake_schema = os.environ.get('SNOWFLAKE_SCHEMA')
snowflake_table = 'FACT_RECORDS'
snowflake_summary_table = 'FACT_SUMMARY'

# Establish Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)

# Streamlit app with tabs
def main():
    st.title("CBN Data Viewer")

    st.sidebar.markdown("### Data Filters")

    # Select region
    region = st.sidebar.selectbox("Select region", ["Bay of Plenty", "Tasman Bay"])
    # translate region to region code
    if region == "Bay of Plenty":
        region = "bop"
    elif region == "Tasman Bay":
        region = "tb"

    # Start date default should be three days before today
    default_start_date = datetime.datetime.now() - datetime.timedelta(days=3)
    start_date = st.sidebar.date_input("Select start date", default_start_date)

    # Select end date
    end_date = st.sidebar.date_input("Select end date", datetime.datetime.now())

    # Fetch summary data from FACT_RECORDS table based on filters
    cursor = conn.cursor()
    query = f"""
        SELECT *
        FROM {snowflake_database}.{snowflake_schema}.{snowflake_summary_table}
        WHERE REGION = '{region}'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    data_summary = pd.DataFrame(rows, columns=columns)
    # Extract date_of_last_record where variable = 'TEMPERATURE' and remove time from the date
    temperature_last_record = data_summary[data_summary['VARIABLE_NAME'] == 'TEMPERATURE']['DATE_OF_LAST_RECORD'].iloc[0].date()
    wind_gust_last_record = data_summary[data_summary['VARIABLE_NAME'] == 'GUST']['DATE_OF_LAST_RECORD'].iloc[0].date()
    wind_speed_last_record = data_summary[data_summary['VARIABLE_NAME'] == 'SPEED']['DATE_OF_LAST_RECORD'].iloc[0].date()
    wind_direction_last_record = data_summary[data_summary['VARIABLE_NAME'] == 'DIRECTION']['DATE_OF_LAST_RECORD'].iloc[0].date()

    # Fetch wind data from FACT_RECORDS table based on filters
    query = f"""
        SELECT TIME, DIRECTION, GUST, SPEED, TEMPERATURE
        FROM {snowflake_database}.{snowflake_schema}.{snowflake_table}
        WHERE REGION = '{region}' AND TIME BETWEEN '{start_date}' AND '{end_date}'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    data = pd.DataFrame(rows, columns=columns)
    
    # Prepare data for wind rose plot
    # Bin the wind directions
    direction_bins = [0, 45, 90, 135, 180, 225, 270, 315, 360]
    direction_labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    # Bin the wind directions
    data['DIRECTION_BIN'] = pd.cut(data['DIRECTION'], bins=direction_bins, labels=direction_labels, include_lowest=True, right=False)

    # Create wind rose plot with classes based on wind speed
    fig = go.Figure()

    # Define wind speed classes and their corresponding ranges
    wind_speed_classes = [
        {'name': '> 47 knots', 'color': 'rgb(106,81,163)', 'range': (47, 3333)},
        {'name': '33-47 knots', 'color': 'rgb(158,154,200)', 'range': (33, 47)},
        {'name': '20-33 knots', 'color': 'rgb(203,201,226)', 'range': (20, 33)},
        {'name': '15-20 knots', 'color': 'rgb(242,240,247)', 'range': (15, 20)},
        {'name': '6-15 knots', 'color': 'rgb(176,196,222)', 'range': (6, 15)},
        {'name': '< 6 knots', 'color': 'rgb(224,255,255)', 'range': (0, 6)}
    ]

    for speed_class in wind_speed_classes:
        # Filter wind data based on speed range
        filtered_data = data[(data['SPEED'] >= speed_class['range'][0]) & (data['SPEED'] < speed_class['range'][1])]
        
        # Aggregate filtered data
        summary = filtered_data.groupby('DIRECTION_BIN').size().reset_index(name='COUNT')
        
        fig.add_trace(go.Barpolar(
            r=summary['COUNT'],
            theta=summary['DIRECTION_BIN'],
            name=speed_class['name'],
            marker_color=speed_class['color']
        ))

    fig.update_layout(
        title='Wind Speed Distribution',
        font_size=16,
        legend_font_size=16,
        polar_radialaxis_ticksuffix=' counts',
        polar_angularaxis_rotation=90,
    )

    # plot temperature
    # Create Plotly line plot for temperature over time
    # remove all records where temperature is null
    data_temp = data.dropna(subset=['TEMPERATURE'])
    # order the data by time
    data_temp = data_temp.sort_values(by='TIME')
    fig_temp = go.Figure()

    fig_temp.add_trace(go.Scatter(
        x=data_temp['TIME'],
        y=data_temp['TEMPERATURE'],
        mode='lines+markers',
        name='Temperature',
        line=dict(color='black', width=1),
        marker=dict(color='darkblue', size=4)
    ))

    fig_temp.update_layout(
        title='Temperature over time',
        xaxis_title='Time',
        yaxis_title='Temperature (°F)',
        legend=dict(font=dict(size=16))
    )

    # Display metric cards
    col1, col2 = st.columns(2)
    col1.metric(f"Latest temperature (Updated at {temperature_last_record.strftime('%Y-%m-%d')})", data_summary[data_summary['VARIABLE_NAME'] == 'TEMPERATURE']['COUNT'].iloc[0])
    col2.metric(f"Latest wind gust (Updated at {wind_gust_last_record.strftime('%Y-%m-%d')})", data_summary[data_summary['VARIABLE_NAME'] == 'GUST']['COUNT'].iloc[0])


    col3, col4 = st.columns(2)
    
    col3.metric(f"Latest wind direction (Updated at {wind_direction_last_record.strftime('%Y-%m-%d')})", data_summary[data_summary['VARIABLE_NAME'] == 'DIRECTION']['COUNT'].iloc[0])
    col4.metric(f"Latest wind speed (Updated at {wind_speed_last_record.strftime('%Y-%m-%d')})", data_summary[data_summary['VARIABLE_NAME'] == 'SPEED']['COUNT'].iloc[0])

    # Display the windrose plot
    st.plotly_chart(fig)

    # Display the temperature plot
    st.plotly_chart(fig_temp)

    # Close the cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
