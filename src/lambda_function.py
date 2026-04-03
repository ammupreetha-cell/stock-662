import json
import requests
import pandas as pd
import boto3
from io import StringIO
import datetime as dt

now = dt.datetime.now()
day = dt.date.today()   
time = now.strftime("%H:%M")

def lambda_handler(event, context):
    try:
        url = "https://api.github.com/repos/squareshift/stock_analysis/contents/"
        response = requests.get(url)
        files = response.json()

        # Collect CSV files
        csv_files = [f["download_url"] for f in files if f["name"].endswith(".csv")]
        sector_file = csv_files.pop()
        sector_df = pd.read_csv(sector_file)

        dataframes = []
        for file_url in csv_files:
            symbol = file_url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(file_url)
            df["Symbol"] = symbol
            dataframes.append(df)

        combined_df = pd.concat(dataframes, ignore_index=True)
        merged_df = pd.merge(combined_df, sector_df, on="Symbol", how="left")

        # Sector aggregation
        result = merged_df.groupby("Sector").agg({
            "open": "mean",
            "close": "mean",
            "high": "max",
            "low": "min",
            "volume": "mean"
        }).reset_index()
        print("\nSector Aggregation:")
        print(result)

        # Filter by timestamp
        merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"])
        filtered_df = merged_df[(merged_df["timestamp"] >= "2021-01-01") &
                                (merged_df["timestamp"] <= "2021-05-26")]

        list_sector = ["TECHNOLOGY", "FINANCE"]
        result_time = filtered_df.groupby("Sector").agg({
            "open": "mean",
            "close": "mean",
            "high": "max",
            "low": "min",
            "volume": "mean"
        }).reset_index()

        result_time = result_time.rename(columns={
            "open": "aggregate_open",
            "close": "aggregate_close",
            "high": "aggregate_high",
            "low": "aggregate_low",
            "volume": "aggregate_volume"
        })

        result_time = result_time[result_time["Sector"].isin(list_sector)]
        print(result_time)

        # Save to S3
        s3 = boto3.client("s3")
        bucket_name = "preetha-stock-api"
        output_key = f"transformed_folder/Date_folder-{day}/transformed_data:{time}.csv"

        csv_buffer = StringIO()
        result_time.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=csv_buffer.getvalue())

        # Save raw JSON metadata
        success_key = f"success_folder/Date_folder-{day}/raw_data:{time}.json"
        json_buffer = StringIO()
        json.dump(files, json_buffer)
        s3.put_object(Bucket=bucket_name, Key=success_key, Body=json_buffer.getvalue())

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Final output saved to s3",
                "s3_path": f"s3://{bucket_name}/{output_key}",
                "preview": result_time.to_dict(orient="records")
            })
        }

    except Exception as e:
        s3 = boto3.client("s3")
        bucket_name = "preetha-stock-api"
        failure_key = f"failure_folder/Date_folder-{day}/message:{time}.json"

        s3.put_object(Bucket=bucket_name, Key=failure_key, Body=str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error occurred, saved to s3",
                "s3_path": f"s3://{bucket_name}/{failure_key}"
            })
        }