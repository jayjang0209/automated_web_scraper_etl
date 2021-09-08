# Automated Web Scraper
Automated data ETL process to extract data from websites and load the data into the databases after transformation 

# Description
## web_scraper_aws_lambda_source_code.py
Serverless Automated Web Scraper that runs on a schedule

Trigger → Serverless function(python script) → Database

    • Trigger - AWS EventBridge(CloudWatch Events)
                schedule AWS Lambda function to run it automatically on a schedule 
                schedule expression: cron(0 3 ? * THU-SAT *)

    • Serverless function - AWS Lambda function with Python
                            Extracts data from web sites, transform the data, and load the data into dynamoDB

    • Database: AWS DynamoDB
                NoSQL database


## web_scraper_load_into_csv_json.py
- Users can run the python script on their local machine 
- Extract data from websites and export data in CSV and JSON formats after data transformation

## web_scraper_load_into_csv_json.py
- Users can run the python script on their local machine 
- Extract data from websites and load data into AWS dynamoDB after data transformation

