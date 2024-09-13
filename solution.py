# Query saved as DataFrame: grocery_sales
# SELECT * FROM grocery_sales

import pandas as pd
import os


def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on="index")
    return merged_df


def transform(raw_data):
    raw_data.fillna(
        {
            "Weekly_Sales": raw_data["Weekly_Sales"].mean(),
            "CPI": raw_data["CPI"].mean(),
            "Unemployment": raw_data["Unemployment"].mean(),
        },
        inplace=True,
    )
    raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="%Y-%m-%d")
    raw_data["Month"] = raw_data["Date"].dt.month
    raw_data = raw_data.loc[raw_data["Weekly_Sales"] > 10000, :]
    return raw_data[
        [
            "Store_ID",
            "Month",
            "Dept",
            "IsHoliday",
            "Weekly_Sales",
            "CPI",
            "Unemployment",
        ]
    ]


def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
    full_data.to_csv(full_data_file_path, index=False)
    agg_data.to_csv(agg_data_file_path, index=False)


def validation(file_path):
    if not (os.path.exists(file_path)):
        raise Exception("Error: Result file not found")


def avg_weekly_sales_per_month(clean_data):
    return (
        clean_data[["Month", "Weekly_Sales"]]
        .groupby("Month")
        .agg(Avg_Sales=("Weekly_Sales", "mean"))
        .reset_index()
        .round(2)
    )


merged_df = extract(grocery_sales, "extra_data.parquet")
clean_data = transform(merged_df)
agg_data = avg_weekly_sales_per_month(clean_data)

load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")
validation("clean_data.csv")
validation("agg_data.csv")
