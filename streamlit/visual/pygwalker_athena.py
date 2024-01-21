import streamlit as st
import awswrangler as wr
import pandas as pd
import boto3
import pygwalker as pyg
import streamlit.components.v1 as stc

boto3.setup_default_session(profile_name='834657444538_Data-Engineering',region_name='eu-west-1')
# Your Streamlit code
def main():

    st.title("Query Athena and Visualize with PyGWalker")

    # User input for the table name
    table_name = st.text_input("Enter the table name")

    if st.button("Fetch and Visualize Data"):
        if table_name:
            try:
                # Construct the SQL query
                query = f"SELECT * FROM {table_name} order by created_at desc limit 1000"
                # Execute query using AWS Wrangler
                df = wr.athena.read_sql_query(query, database="ai_monitoring",ctas_approach=False)
                #st.dataframe(df)
                # Use PyGWalker to visualize the data
                pyg_html = pyg.walk(df, return_html=True)
                stc.html(pyg_html, scrolling=True, height=1000)

            except Exception as e:
              st.error(f"An error occurred: {e}")
        else:
            st.error("Please enter a table name")

if __name__ == "__main__":
    main()
