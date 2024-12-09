import pandas as pd
import sqlite3


def extract_data(file_path):
    """Reads data from a CSV file."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"Error reading file: {e}")


def validate_data(df):
    """Checks for missing values and ensures the required columns exist."""
    required_columns = [
        "year",
        "make",
        "model",
        "trim",
        "body",
        "vin",
        "state",
        "odometer",
        "color",
        "interior",
        "seller",
        "mmr",
        "sellingprice",
        "saledate",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    df = df.dropna()
    return df


def convert_dates_to_standard_format(df):
    """Converts dates in the specified column to a standard format."""
    df["saledate"] = pd.to_datetime(df["saledate"], errors="coerce")
    return df.dropna(subset=["saledate"])  # Drop rows with invalid dates


def aggregate_sales_by_category(df):
    """Aggregates sales by category (make and model)."""
    aggregated_df = df.groupby(["make", "model"], as_index=False).agg(
        {
            "sellingprice": "mean",  # Average selling price
            "odometer": "mean",  # Average odometer
        }
    )
    # Rename columns for clarity
    aggregated_df.rename(
        columns={
            "sellingprice": "avg_sellingprice",
            "odometer": "avg_odometer",
        },
        inplace=True,
    )
    return aggregated_df


def save_data_to_csv(df, output_path):
    """Saves the transformed data to a CSV file."""
    try:
        df.to_csv(output_path, index=False)
    except Exception as e:
        raise ValueError(f"Error saving CSV: {e}")


def save_data_to_sqlite(df, db_path, table_name):
    """Saves the transformed data to an SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
    except Exception as e:
        raise ValueError(f"Error saving to SQLite: {e}")


if __name__ == "__main__":
    raw_data_path = "data/car_prices.csv"
    converted_csv_path = "processed_data/converted_car_prices.csv"
    aggregated_csv_path = "processed_data/aggregated_car_prices.csv"
    db_path = "processed_data/car_prices.db"

    print("Loading data...")
    data = extract_data(raw_data_path)

    print("Validating data...")
    data = validate_data(data)

    print("Converting saledate to standard format...")
    data = convert_dates_to_standard_format(data)

    print("Saving converted data...")
    save_data_to_csv(data, converted_csv_path)
    save_data_to_sqlite(data, db_path, "converted_date_format")
    print("Converted saledate to standard format!")

    print("Aggregating sales by category...")
    aggregated_data = aggregate_sales_by_category(data)

    print("Saving aggregated data...")
    save_data_to_csv(aggregated_data, aggregated_csv_path)
    save_data_to_sqlite(aggregated_data, db_path, "car_summary")

    print("Pipeline executed successfully!")
