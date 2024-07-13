import streamlit as st
import snowflake.connector
import os
from dotenv import load_dotenv
import datetime

load_dotenv()

# Snowflake connection parameters
snowflake_user = os.environ.get('SNOWFLAKE_USER')
snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')
snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
print(snowflake_warehouse)
snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
snowflake_schema = os.environ.get('SNOWFLAKE_SCHEMA')
snowflake_table = 'FACT_RECORDS'

# Establish Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)

# Fetch column names from fact_records table
cursor = conn.cursor()
cursor.execute(f"DESCRIBE TABLE {snowflake_table}")
rows = cursor.fetchall()
columns = [row[0] for row in rows]

# Fetch the first row from fact_records table
cursor.execute(f"SELECT * FROM {snowflake_table} LIMIT 1")
first_row = cursor.fetchone()

# Close the cursor and connection
conn.close()

# Streamlit app with tabs
def main():
    st.title("Snowflake Data Viewer")

    tabs = st.tabs(["Data summary", "Data exploration"])

    with tabs[0]:  # View First Row tab
        st.header("First Row in fact_records table:")
        st.write(first_row)

    with tabs[1]:  # Select Data Types tab
        st.header("Select Data Types for Columns")
        st.write("Choose columns from your fact_records table and select data types for each.")

        # Allow user to select columns
        selected_columns = st.multiselect("Select Columns", columns)
        
        # select start date
        start_date = st.date_input("Select start date", datetime.date(2019, 7, 6))

        # select end date
        end_date = st.date_input("Select end date", datetime.date(2019, 7, 6))



        # Dictionary to map column names to selected data types
        # data_types = {}
        # for column in selected_columns:
        #     data_types[column] = st.selectbox(f"Select data type for column '{column}'", ['TEXT', 'INTEGER', 'FLOAT', 'DATE'])

        # # Display selected data types
        # st.write("Selected Data Types:")
        st.write(selected_columns)

if __name__ == "__main__":
    main()
