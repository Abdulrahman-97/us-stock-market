from dotenv import load_dotenv 
import os
import pandas as pd
import numpy as np
import s3fs
import fastparquet as fp
from tickers_utils import calc_indicators

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIX_1D = os.getenv("PREFIX_1D")
FUNDAMENTAL_DATA = os.getenv("FUNDAMENTAL_DATA")

s3 = s3fs.S3FileSystem(anon=False)

def read_s3(file, bucket_name=BUCKET_NAME, prefix=PREFIX_1D, file_format='csv'):
    file_prefix = prefix+file
    file_path = f's3://{bucket_name}/{file_prefix}'
    if s3.exists(file_path):
        with s3.open(file_path, 'rb') as s3_file:
            if file_format == 'csv':
                df = pd.read_csv(s3_file, na_filter = False)
            elif file_format == 'fastparquet':
                df = fp.ParquetFile(s3_file)
            else:
                df = pd.read_parquet(s3_file)
            return df
    else:
        print(f"File path not found")

def write_to_s3(df, file_name, bucket_name=BUCKET_NAME, prefix=FUNDAMENTAL_DATA, file_format='csv'):
    file_prefix = prefix+file_name
    file_path = f's3://{bucket_name}/{file_prefix}'
    if file_format == 'csv':
        with s3.open(file_path,'w') as f:
            df.to_csv(f)
    else:
        fp.write(
            file_path,
            df,
            compression='GZIP',
            open_with=s3.open,
            stats=True
        )

def ListFilesS3(bucket_name=BUCKET_NAME, prefix=PREFIX_1D, full_path=False):
    """List files in specific S3 URL"""
    tickers = []
    fs = s3fs.S3FileSystem()
    path = f"s3://{bucket_name}/{prefix}"

    #files references the entire bucket.
    files = fs.ls(path)
    for content in files:
        if not full_path:
            content = content[content.find("/", content.find("/")+1, content.find("."))+1:]
        tickers.append(content)
    return tickers[1:]

def create_update_parquet(file_name, data, bucket_name=BUCKET_NAME, prefix=PREFIX_1D):
    s3 = s3fs.S3FileSystem(anon=False)

    s3_bucket = bucket_name
    s3_key = f'{file_name}.parquet'
    s3_prefix = prefix
    s3_path = f's3://{s3_bucket}/{s3_prefix}{s3_key}'

    additional_columns = ['bb_bbm', 'bb_bbh', 'bb_bbl', 'bb_bbhi', 'bb_bbli', 'bb_bbw', 'bb_bbp', 'mfi'
     'adi', 'sma_200', 'sma_50', 'macd', 'macd_diff', 'macd_signal', 'adx', 'adx_neg'
     'adx_pos', 'rsi', 'stoch', 'stoch_signal', 'solid_trend', 'golden_cross',
     'price_over_200', 'macd_up', 'rsi_over', 'rsi_under', 'adx_buy', 'adx_sell',
     'stoch_over', 'stoch_under']
    
    if s3.exists(s3_path):       
        # Read the existing Parquet file into a DataFrame
        with s3.open(s3_path, 'rb') as s3_file:
                parquet_file = fp.ParquetFile(s3_file)
                existing_df = parquet_file.to_pandas(parquet_file.columns[:13])

        new_df = pd.DataFrame(data['historical']).sort_values('date').reset_index(drop=True)

        # Concatenate the new data with the existing DataFrame
        merged = pd.concat([existing_df.reset_index(drop=True), new_df.reset_index(drop=True)])
        merged = merged.drop_duplicates(['date'], keep="last").reset_index(drop=True)

        # Calculate all tech indicators
        if len(merged) >= 50:
            merged = calc_indicators(merged)
        else:
            merged[additional_columns] = [np.nan for i in additional_columns]

        # Write the updated DataFrame to the appended Parquet file on S3 using fastparquet
        fp.write(
            s3_path,
            merged,
            compression='GZIP',
            open_with=s3.open,
            stats=True
        )
    else:
        # Create a Pandas DataFrame with your data
        df = pd.DataFrame(data['historical']).sort_values('date').reset_index(drop=True)

        if len(df) >= 50:
            df = calc_indicators(df)
        else:
            df[additional_columns] = [np.nan for i in additional_columns]

        # Write the DataFrame to the new Parquet file on S3 using fastparquet
        fp.write(
            s3_path,
            df,
            compression='GZIP',
            open_with=s3.open,
            stats=True
        )