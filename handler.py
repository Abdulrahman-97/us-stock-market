import json
from fmp import FMP
from aws_utils import read_s3, write_to_s3
from tickers_utils import calc_fundamental
import boto3
import time
from datetime import datetime, timedelta


fmp = FMP()

def create_update_ticker(event, context):
    if isinstance(event, list):
        for ticker in event:
            symbol = ticker['symbol']
            date_from = ticker['last_updated']
            fmp.create_update_ticker(symbol, date_from=date_from)
    else:
        symbol = event['symbol']
        date_from = event['last_updated']
        fmp.create_update_ticker(symbol, date_from=date_from)

    response = {
        "statusCode": 200,
        # "body": json.dumps(body)
    }
    return response

def get_all_tickers_from_s3(event, context):
    date_from = (datetime.today() + timedelta(-3)).strftime('%Y-%m-%d')
    starting_id = int(event['symbols']['starting_id'])
    tickers = fmp.get_all_tickers_from_s3(starting_id, sublist_size=10, initial_load=True, date_from=date_from)

    starting_id += 299
    response = {
        "statusCode": 200,
        "starting_id": starting_id,
        "body": json.dumps(tickers)
    }

    return response


def load_fundamental_data_to_s3(event, context):
    
    metrics = fmp.get_metrics_bulk()
    ratios = fmp.get_ratios_bulk()
    scores = fmp.get_scores_bulk()
    ratings = fmp.get_ratings_bulk()

    write_to_s3(metrics, 'key_metrics_ttm.parquet', file_format='parquet')
    write_to_s3(ratios, 'ratios_ttm.parquet', file_format='parquet')
    write_to_s3(scores, 'scores.parquet', file_format='parquet')
    write_to_s3(ratings, 'ratings.parquet', file_format='parquet')

def run_athena_query(event, context):

    client = boto3.client('athena', region_name='us-east-1')
    last_date = read_s3('AAPL.parquet', file_format='fastparquet').statistics['max']['date'][0]

    queryStart = client.start_query_execution(
        QueryString = f"SELECT * FROM awsdatacatalog.stocks.tickers_view where date = '{last_date}';",
        QueryExecutionContext = {
            'Database': 'stocks',
            'Catalog': 'AwsDataCatalog'
        }, 
        ResultConfiguration = { 'OutputLocation': 's3://stockbucketus/athena_result/'}
    )
    queryExecution = client.get_query_execution(QueryExecutionId=queryStart['QueryExecutionId'])
    file_name = queryExecution['QueryExecution']['ResultConfiguration']['OutputLocation'].split('/')[-1]
    status = queryExecution['QueryExecution']['Status']['State']

    start_time = time.time()
    while status != 'SUCCEEDED':
        if (time.time() - start_time) > 10:
            break
        
    return {
        "status": status,
        "file_name": file_name
    }

def merge_screener_data(event, context):

    file_name = event['file_name']

    tickers_ohlc = read_s3(file_name, prefix='athena_result/', file_format='csv')
    tickers_ohlc = tickers_ohlc.rename(columns={'ticker':'symbol'})

    ratios = read_s3('ratios_ttm.parquet', prefix='fundamental_data/', file_format='parquet')[['symbol', 'peRatioTTM', 'pegRatioTTM', 'netProfitMarginTTM', 'priceFairValueTTM']]
    scores = read_s3('scores.parquet', prefix='fundamental_data/', file_format='parquet')[['symbol', 'altmanZScore', 'piotroskiScore']]
    ratings = read_s3('ratings.parquet', prefix='fundamental_data/', file_format='parquet')[['symbol', 'rating', 'ratingScore', 'ratingDetailsDCFScore', 'ratingDetailsROEScore', 'ratingDetailsROAScore', 'ratingDetailsDEScore', 'ratingDetailsPEScore', 'ratingDetailsPBScore']]
    profiles = read_s3('companies_profiles.parquet', prefix='fundamental_data/', file_format='parquet')[['symbol', 'price', 'mktCap', 'range', 'companyName', 'industry', 'sector', 'exchangeShortName']]
    profiles[['price_low_ttm', 'price_high_ttm']] = profiles['range'].str.split('-', expand=True)[[0, 1]]

    fundamental_screener = profiles.merge(ratios, how='left').merge(scores, how='left').merge(ratings, how='left')
    fundamental_screener = calc_fundamental(fundamental_screener)

    all_df = tickers_ohlc.merge(fundamental_screener, on='symbol')

    write_to_s3(all_df, 'merged_screener_all.parquet', file_format='parquet')




