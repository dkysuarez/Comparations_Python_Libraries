import pandas as pd
import numpy as np
import streamlit as st
import time
import psutil

# Function to measure memory usage
def measure_memory():
    return psutil.Process().memory_info().rss / (1024 * 1024)   # Memory in MB

# Application title
st.title("Comparison of Data Processing Libraries")

 # Option to upload CSV files
uploaded_file = st.file_uploader(" Upload a CSV file", type=["csv"])

# Read the CSV file if uploaded
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write(" Loaded data:")
    st.dataframe(df.head())  # Show the first rows of the DataFrame

    # Get available columns
    columns = df.columns.tolist()

    # Select column to operate on
    selected_column = st.selectbox("Select a column:", columns)

    # Validate that a column has been selected
    if selected_column:
        # Check if the column is numeric
        if not pd.api.types.is_numeric_dtype(df[selected_column]):
            st.error(f"The column  '{selected_column}' is not numeric. Please select another column.")
        else:
            options = ["Pandas", "Polars", "Dask", "Modin", "Swifter", "Cython"]
            selected_library = st.selectbox("Select a library:", options)

            operation = st.selectbox("Select an operation:", ["Sum", "Average"])

            if st.button("Run Test"):
                # Measure memory before the operation
                initial_memory = measure_memory()
                
                start_time = time.time()
                
                try:
                    if selected_library == "Pandas":
                        result = df[selected_column].sum() if operation == "Sum" else df[selected_column].mean()
                    elif selected_library == "Polars":
                        import polars as pl
                        pl_df = pl.from_pandas(df)
                        result = pl_df[selected_column].sum() if operation == "Sum" else pl_df[selected_column].mean()
                    elif selected_library == "Dask":
                        import dask.dataframe as dd
                        ddf = dd.from_pandas(df, npartitions=4)
                        result = ddf[selected_column].sum().compute() if operation == "Sum" else ddf[selected_column].mean().compute()
                    elif selected_library == "Modin":
                        import modin.pandas as mpd
                        modin_df = mpd.DataFrame(df)
                        result = modin_df[selected_column].sum() if operation == "Sum" else modin_df[selected_column].mean()
                    elif selected_library == "Swifter":
                        import swifter
                        result = df[selected_column].swifter.apply(lambda x: x.sum() if operation == "Sum" else x.mean())


                    duration = time.time() - start_time
                    final_memory = measure_memory()
                    
                    st.write(f"Result: {result}")
                    st.write(f"Execution time: {duration:.4f} seconds")


                    st.write(f"Initial memory usage: {initial_memory:.2f} MB")
                    st.write(f"Final memory usage: {final_memory:.2f} MB")
                    
                    #  Save results in a DataFrame to display in a table
                    results = {
                        "Library": selected_library,
                        "Operation": operation,
                        "Column": selected_column,
                        "Result": result,
                        "Time (s)": duration,
                        "Initial Memory(MB)": initial_memory,
                        "Final Memory(MB)":  final_memory,
                    }
                    
                    results_df = pd.DataFrame([results])
                    st.write(results_df)

                except Exception as e:
                    st.error(f"An error occurred during execution: {str(e)}")
    else:
        st.warning("Please select a column to operate on.")
else:
    st.warning("Please upload a CSV file.")