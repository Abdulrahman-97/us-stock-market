# Serverless Data Pipelines for US Stock Martket in AWS
-----------------
This project is designed to serve a data pipeline for the US stock market. The data is fetched from Financial Modeling Prep and stored in S3.

## Archeticture
![AWS_diagram](https://github.com/Abdulrahman-97/us-stock-market/assets/58907996/d2737d3b-c042-4f34-bc02-2552f5005a7a)

#### Storage 
- S3
- Parquet

All data be it market data i.e. tickers or fundamental data are stored as parquet files on S3. Each ticker is stored in its own parquet file. So, there are over 9k files each one representing a ticker. 

#### Processing
- AWS Lambda
- AWS Athena

AWS Lambda serves as a compute layer for processing and transforming data. Athena is used to scan all ticker files and collect the last record on each file to be merged all in one file. This will the serve the screener page.

#### Orchestration
- AWS Step Functions

AWS Step Functions is used to orchestrate the needed Lambda functions.

![stepfunctions_graph](https://github.com/Abdulrahman-97/us-stock-market/assets/58907996/3880b839-51e5-476a-a4cb-c480d0c8aac9)


##### Lambda functions
- FetchFileName: It will fetch all file names to be updated from S3
- ProcessFilesInParallel: Since the API used has a qouta of 300 requests/min, we only process 300 files at a time.
    - ProcessFile: It will do the necessary transformations to the requested ticker and update the file in S3
- Wait state: The pipeline will pause for 60 seconds
- Choice state: It will check if all files are processed, otherwise it will continute processing the remaining files.
- fetchFundamentalData: It will fetch fundamental data for all tickers
- runAthenaQuery: It will run an Athena query to scan all ticker files and collect the last record on each file to be merged all in one file.
- mergeScreenerData: It will merge market and fundamental data.

#### Scheduleing
- AWS Eventbridge scheduler

To schedule the state machine, we used AWS Eventbridge scheduler and set up a scheduled event on a daily basis when the market is open.
