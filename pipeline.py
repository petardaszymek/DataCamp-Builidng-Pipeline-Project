import pandas as pd
import os

# Function to extract data from two sources and merge them based on the "index" column
def extract(basic_data, extra_data):
    # Read the extra data from a Parquet file
    extra_df = pd.read_parquet(extra_data)
    # Merge basic data with extra data based on the "index" column
    merged_df = basic_data.merge(extra_df, on="index")
    return merged_df

# Extract data from grocery_sales and extra_data.parquet
merged_df = extract(grocery_sales, "extra_data.parquet")

# Function to clean and preprocess the data
def transform(input_data):
    # Fill missing values in specific columns with their mean values
    input_data.fillna({
        'CPI': input_data['CPI'].mean(),
        'Weekly_Sales': input_data['Weekly_Sales'].mean(),
        'Unemployment': input_data['Unemployment'].mean(),
    }, inplace=True)
    
    # Convert the "Date" column to datetime format and extract the month
    input_data["Date"] = pd.to_datetime(input_data["Date"], format="%Y-%m-%d")
    input_data["Month"] = input_data["Date"].dt.month
    
    # Filter out rows with "Weekly_Sales" less than or equal to 10000
    input_data = input_data[input_data["Weekly_Sales"] > 10000]
    
    # Select specific columns for further analysis
    input_data = input_data[["Store_ID", "Month", "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment"]]
    return input_data

# Clean and preprocess the merged data
clean_data = transform(merged_df)

# Function to calculate the average weekly sales for each month
def avg_monthly_sales(clean_data):
    # Extract relevant columns for aggregation
    holidays_sales = clean_data[["Month", "Weekly_Sales"]]
    # Group by month and calculate the mean of weekly sales
    holidays_sales = holidays_sales.groupby("Month")["Weekly_Sales"].mean().reset_index(name="Avg_Sales").round(2)
    return holidays_sales

# Aggregate the cleaned data to calculate average monthly sales
agg_data = avg_monthly_sales(clean_data)

def load(clean_data, clean_data_file_path, agg_data, agg_data_file_path):
    # Save cleaned data to CSV
    clean_data.to_csv(clean_data_file_path, index=False)
    # Save aggregated data to CSV
    agg_data.to_csv(agg_data_file_path, index=False)

# Save cleaned and aggregated data to CSV files
load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")


# Function to validate the existence of a file at a given path
def validation(file_path):
    # Check if the file exists
    file_exists = os.path.exists(file_path)
    if not file_exists:
        print(f"There is no file at the path {file_path}")
    else:
        print(f"File at the path {file_path} exists.")

# Validate the existence of the cleaned and aggregated data files
validation("clean_data.csv")
validation("agg_data.csv")
