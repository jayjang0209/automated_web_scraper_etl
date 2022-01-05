"""

Serverless Automated Web Scraper - Data ETL (Extract, Transform, Load)

    Trigger: AWS EventBridge(CloudWatch Events)
            Schedule expression: cron(0 3 ? * THU-SAT *)

    Serverless function: AWS Lambda function
                        Extracts data from web site, transform data, and load data dynamoDB

    Database: AWS DynamoDB
              NoSQL database

    test ongoing
"""

import json
import boto3
from bs4 import BeautifulSoup
import requests
import pandas as pd
from dateutil.parser import parse

URL = "https://www.canada.ca/en/immigration-refugees-citizenship/corporate/mandate/policies-operational-instructions-agreements/ministerial-instructions/express-entry-rounds.html"
DATA_COLUMNS = ["date", "program", "invitations", "lowest_crs"]


def lambda_handler(event, context):
    html_data = requests.get(URL).text

    soup = BeautifulSoup(html_data, "html.parser")

    # Create empty dataframe

    data_extracted = pd.DataFrame(columns=DATA_COLUMNS)
    for row in soup.find_all('tbody')[0].find_all('tr'):
        col = row.find_all('td')
        if col:
            date = parse(col[1].text).strftime('%Y-%m-%d')
            program = col[2].text
            invitations = int(col[3].text.replace(',', ''))
            lowest_crs = int(col[4].text)
            data_extracted = data_extracted.append(
                {"date": date, "program": program, "invitations": invitations, "lowest_crs": lowest_crs},
                ignore_index=True)

    # Connect to dynamoDB

    dynamodb = boto3.resource('dynamodb')  # Connect to dynamodb from aws lambda function
    table = dynamodb.Table('CRS_history')  # specify the target table

    # Insert items into dynamoDB

    with table.batch_writer() as batch:
        for i in range(len(data_extracted)):
            batch.put_item(
                Item={
                    'program': data_extracted['program'][i],
                    'date': data_extracted['date'][i],
                    'invitations': data_extracted['invitations'][i],
                    'lowest_crs': data_extracted['lowest_crs'][i]
                }
            )

    return {
        'statusCode': 200,
        'body': json.dumps("updated completed")
    }
