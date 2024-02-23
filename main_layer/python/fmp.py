import requests
import asyncio
import aiohttp
import pandas as pd
from itertools import chain
from aws_utils import *
from dotenv import load_dotenv 
import time


load_dotenv()

_BASE_URL_ = 'https://financialmodelingprep.com'

COMPANY_RATING = '/api/v3/rating/'
SYMBOLS_LIST = '/api/v3/stock/list'
HISTORICAL_PRICE = '/api/v3/historical-price-full/'
COMPANY_OUTLOOK = '/api/v4/company-outlook'
COMPANY_RATING_BULK = '/api/v4/rating-bulk'
COMPANY_KEY_METRICS_BULK = '/api/v4/key-metrics-ttm-bulk'
COMPANY_RATIOS_BULK = '/api/v4/ratios-ttm-bulk'
COMPANY_SCORES_BULK = '/api/v4/scores-bulk'

class FMP:

    def __init__(self, api_key=os.getenv("API_KEY")):
        self.__api_key = api_key

    def get_all_symbols(self):
        url = _BASE_URL_+SYMBOLS_LIST+'?'+'apikey='+self.__api_key
        r = requests.get(url)
        return r.json()
    
    def get_company_outlook_data(self, symbol):
        url = _BASE_URL_+COMPANY_OUTLOOK+'?'+'symbol='+symbol+'&apikey='+self.__api_key
        r = requests.get(url)
        return r.json()

    def get_rating(self, symbol):
        url = _BASE_URL_+COMPANY_RATING+symbol+'?'+'apikey='+self.__api_key
        r = requests.get(url)
        return r.json()
    
    def get_rating_url(self, symbol):
        url = _BASE_URL_+COMPANY_RATING+symbol+'?'+'apikey='+self.__api_key
        return url

    def get_historical_price(self, *args, **kwargs):
        return self.request_endpoint(HISTORICAL_PRICE, *args, **kwargs)

    def request_endpoint(self, endpoint, symbol, timeseries=None, date_from=None, date_to=None, return_urls=False):
        if timeseries:
            url = _BASE_URL_+endpoint+symbol+'?'+'timeseries='+timeseries+'&'+'apikey='+self.__api_key
        elif date_from and date_to:
            try:
                url = _BASE_URL_+endpoint+symbol+'?'+'from='+date_from+'&'+'to='+date_to+'&'+'apikey='+self.__api_key
            except:
                print(symbol)
        elif date_from:
            try:
                url = _BASE_URL_+endpoint+symbol+'?'+'from='+date_from+'&'+'apikey='+self.__api_key
            except:
                print(symbol)
        else:
            url = _BASE_URL_+endpoint+symbol+'?'+'apikey='+self.__api_key
        
        if return_urls:
            return url
        try:
            r = requests.get(url)
        except Exception as e:
            # Handle other unexpected exceptions
            print(f"An unexpected error occurred: {e}")
        return r.json()
    
    def request_bulk_endpoint(self, endpoint):
        url = _BASE_URL_+endpoint+'?'+'apikey='+self.__api_key
        try:
            r = pd.read_csv(url)
        except Exception as e:
            # Handle other unexpected exceptions
            print(f"An unexpected error occurred: {e}")
        return r
    
    def get_ratings_bulk(self):
        return self.request_bulk_endpoint(COMPANY_RATING_BULK)
    
    def get_scores_bulk(self):
        return self.request_bulk_endpoint(COMPANY_SCORES_BULK)
    
    def get_metrics_bulk(self):
        return self.request_bulk_endpoint(COMPANY_KEY_METRICS_BULK)
    
    def get_ratios_bulk(self):
        return self.request_bulk_endpoint(COMPANY_RATIOS_BULK)
    
    def get_all_tickers_urls(self, symbols=None, multiple=False, timeseries=False, *args, **kwargs):
        if len(symbols):
            symbols_list = symbols['symbol'].to_list()
        else:
            all_symbols_df = pd.read_csv('tickers.csv', na_filter = False)
            symbols_list = all_symbols_df[(all_symbols_df['exchangeShortName'] == 'NASDAQ') & (all_symbols_df['type'] == 'stock')]['symbol'].to_list()
        if multiple:
            if timeseries:
                urls = [self.get_historical_price(','.join(symbols_list[i:i+5]), *args, **kwargs, return_urls=True, timeseries=timeseries) for i in range(0, len(symbols_list), 5)]
            else:    
                urls = [self.get_historical_price(','.join(symbols_list[i:i+3]), *args, **kwargs, return_urls=True) for i in range(0, len(symbols_list), 3)]
        else:
            urls = [self.get_historical_price(symbol, *args, **kwargs, return_urls=True) for symbol in symbols_list]

        return urls
    
    def get_tickers_urls_with_dates(self, symbols_with_dates=None, multiple=False, timeseries=False, *args, **kwargs):
        
        if multiple:
            if timeseries:
                urls = [self.get_historical_price(','.join(symbols_with_dates[i:i+5]), *args, **kwargs, return_urls=True, timeseries=timeseries) for i in range(0, len(symbols_with_dates), 5)]
            else:    
                urls = [self.get_historical_price(','.join(symbols_with_dates[i:i+3]), *args, **kwargs, return_urls=True) for i in range(0, len(symbols_with_dates), 3)]
        else:
            urls = [self.get_historical_price(symbol_dict['symbol'],  *args, **kwargs, return_urls=True, date_from=symbol_dict['last_updated']) for symbol_dict in symbols_with_dates]

        return urls
    
    def get_tickers_urls(self, symbols=None, multiple=False, *args, **kwargs):
        if len(symbols):
            symbols_list = symbols['symbol'].to_list()
        else:
            all_symbols_df = pd.read_csv('tickers.csv', na_filter = False)
            symbols_list = all_symbols_df[(all_symbols_df['exchangeShortName'] == 'NASDAQ') & (all_symbols_df['type'] == 'stock')]['symbol'].to_list()
        if multiple:
            urls = [self.get_historical_price(','.join(symbols_list[i:i+3]), *args, **kwargs, return_urls=True) for i in range(0, len(symbols_list), 3)]
        else:
            urls = [self.get_historical_price(symbol, *args, **kwargs, return_urls=True) for symbol in symbols_list]

        return urls

    def test_multiple_tickers(self, symbols=None, multiple=False, *args, **kwargs):
        if symbols:
            symbols_list = symbols['symbol'].to_list()
        else:
            all_symbols_df = pd.read_csv('tickers.csv', na_filter = False)
            symbols_list = all_symbols_df[(all_symbols_df['exchangeShortName'] == 'NASDAQ') & (all_symbols_df['type'] == 'stock')][:100]['symbol'].to_list()
        if multiple:
            urls = self.get_historical_price(','.join(symbols_list), *args, **kwargs)
        else:
            urls = [self.get_historical_price(symbol, *args, **kwargs, return_urls=True) for symbol in symbols_list]

        return urls
    
    async def fetch(self, session, url):
        count = 0
        flag = True
        while flag:
            try:
                async with session.get(url, ssl=False) as response:
                    res = await response.json()
                    if res == []:
                        count += 1
                    elif count > 2:
                        return []
                    else:
                        flag = False
                        return res
            except:
                asyncio.sleep(0)

    async def get_responses(self, urls):
        async with aiohttp.ClientSession() as session:            
            responses = await asyncio.gather(*[self.fetch(session, url) for url in urls])
            return responses

    def get_all_tickers(self, multiple=False,*args, **kwargs):
        chnum = 300
        urls = self.get_all_tickers_urls(*args, **kwargs)
        chunks = [urls[x:x+chnum] for x in range(0, len(urls), chnum)]
        res = []
        for chunk_urls in chunks:
            result = asyncio.run(self.get_responses(chunk_urls))

            if multiple:
                [pd.DataFrame(r['historicalStockList'][0]['historical']).to_parquet(f'tickers/{r["historicalStockList"][0]["symbol"]}.parquet', engine='fastparquet') for r in result]
                [pd.DataFrame(r['historicalStockList'][1]['historical']).to_parquet(f'tickers/{r["historicalStockList"][1]["symbol"]}.parquet', engine='fastparquet') for r in result]
                [pd.DataFrame(r['historicalStockList'][2]['historical']).to_parquet(f'tickers/{r["historicalStockList"][2]["symbol"]}.parquet', engine='fastparquet') for r in result]
            else:
                [pd.DataFrame(r['historical']).to_parquet(f'tickers/{r["symbol"]}.parquet', engine='fastparquet') for r in result]
            if len(chunks) > 1:
                time.sleep(61)
        return res
    
    def load_all_tickers(self, multiple=False,*args, **kwargs):
        chnum = 300
        urls = self.get_all_tickers_urls(*args, **kwargs)
        chunks = [urls[x:x+chnum] for x in range(0, len(urls), chnum)]
        res = []
        for chunk_urls in chunks:
            result = asyncio.run(self.get_responses(chunk_urls))
            if multiple:
                [pd.DataFrame(r['historicalStockList'][0]['historical']).to_parquet(f'tickers/{r["historicalStockList"][0]["symbol"]}.parquet', engine='fastparquet') for r in result]
                [pd.DataFrame(r['historicalStockList'][1]['historical']).to_parquet(f'tickers/{r["historicalStockList"][1]["symbol"]}.parquet', engine='fastparquet') for r in result]
                [pd.DataFrame(r['historicalStockList'][2]['historical']).to_parquet(f'tickers/{r["historicalStockList"][2]["symbol"]}.parquet', engine='fastparquet') for r in result]
            else:
                [pd.DataFrame(r['historical']).to_parquet(f'tickers/{r["symbol"]}.parquet', engine='fastparquet') for r in result]
            if len(chunks) > 1:
                time.sleep(61)
        return res

    def convert_to_list_of_lists(self, original_list, sublist_size):
        # Using list comprehension to create sublists
        list_of_lists = [original_list[i:i+sublist_size] for i in range(0, len(original_list), sublist_size)]
        return list_of_lists
    
    def get_all_tickers_from_s3(self, starting_id, initial_load=False, sublist_size=False, date_from='1900-01-01'):
        if initial_load:
            tickers = read_s3('all_symbols.csv', prefix='')
            tickers = tickers[tickers['Id'].between(starting_id, starting_id+299)]
            tickers = tickers['symbol'].to_list()
            tickers_last_dates = [{'symbol': ticker, 'last_updated': date_from} for ticker in tickers]
            if sublist_size:
                tickers_last_dates = self.convert_to_list_of_lists(tickers_last_dates, sublist_size)
        else:
            tickers = ListFilesS3(full_path=False)
        # print(tickers)
            tickers_last_dates = [{'symbol': ticker[ticker.find("/")+1:ticker.find(".")], 'last_updated': read_s3(ticker, file_format='fastparquet').statistics['max']['date'][0]} for ticker in tickers]
            if sublist_size:
                tickers_last_dates = self.convert_to_list_of_lists(tickers_last_dates, sublist_size)
        return tickers_last_dates
    
    def test_tickers(self, multiple=False, timeseries=False, *args, **kwargs):
        start_time = time.time()
        chnum = 300
        tickers = read_s3('symbols.csv')
        urls = self.get_all_tickers_urls(tickers[:100], multiple=multiple, timeseries=timeseries, *args, **kwargs)
        print(time.time() - start_time)
        start_time = time.time()
        print(start_time)
        result = asyncio.run(self.get_responses(urls))
        print("Getting responses")
        print(time.time() - start_time)

        return result
    # This function can be used in the invoked AWS Lambda function which takes ticker symbol and last_date.
    # It fetches the ticker data first given the date and then updates the parquet file in S3
    def create_update_ticker(self, symbol, date_from=False):
        if not date_from:
            data = self.get_historical_price(symbol)
        else:
            data = self.get_historical_price(symbol, date_from=date_from)
        try:
            create_update_parquet(symbol, data)
        except Exception as e:
            return e

        return data


    
