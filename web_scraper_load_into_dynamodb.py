"""
Web scraper - Data ETL (Extract, Transform, Load)
Import the extracted data into dynamodb
"""

from bs4 import BeautifulSoup
import boto3
import requests
from dateutil.parser import parse
import pandas as pd
from datetime import datetime

URL = "https://www.canada.ca/en/immigration-refugees-citizenship/corporate/mandate/policies-operational-instructions-agreements/ministerial-instructions/express-entry-rounds.html"
DATA_COLUMNS = ["date", "program", "invitations", "lowest_crs"]
AWS_ACCESS_KEY_ID = 'your_aws_access_key'
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_key'


def extract_from_web_data(url):
    """Extract data from web and return the data in a pandas DataFrame.

    :param url: an url of website
    :precondition: url must be a valid address of a web page
    :postcondition: returns the extracted data from the url in a pandas DataFrame
    :return: pandas DataFrame file of extracted data
    """
    html_data = requests.get(url).text
    soup = BeautifulSoup(html_data, "html5lib")  # Create bs object
    data = pd.DataFrame(columns=DATA_COLUMNS)  # Create empty dataframe

    for row in soup.find_all('tbody')[0].find_all('tr'):  # index of target table: 0
        col = row.find_all('td')
        if col:
            date = col[1].text
            program = col[2].text
            invitations = col[3].text
            lowest_crs = col[4].text
            data = data.append({"date": date, "program": program, "invitations": invitations, "lowest_crs": lowest_crs},
                               ignore_index=True)

    return data


def transform(data):
    """Change date format of values of date and remove 1000's separator from values of invitations.

    :param data: a pandas dataFrame object
    :precondition: data must be a pandas dataFrame object
    :postcondition: returns transformed data
    :return: returns transformed data
    """
    data['date'] = data['date'].apply(lambda date: parse(date).strftime('%Y-%m-%d'))
    data['invitations'] = data['invitations'].str.replace(',', '').astype(int)
    data['lowest_crs'] = data['lowest_crs'].astype(int)
    return data


def import_data_into_dynamodb(data):
    """Import the extracted data into AWS dynamoDB.

    :param data: pandas dataFrame object
    :precondition: data must be a pandas dataFrame object
    :postcondition: imports the extracted data into AWS dynamoDB
    """
    dynamodb = boto3.resource('dynamodb', region_name='ca-central-1', aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table('CRS_history')

    with table.batch_writer() as batch:
        for i in range(len(data)):
            batch.put_item(
                Item={
                    'program': data['program'][i],
                    'date': data['date'][i],
                    'invitations': int(data['invitations'][i]),
                    'lowest_crs': int(data['lowest_crs'][i])
                }
            )
        print('done')


def log(message):
    """Log ETL process

    :param message: a string
    :precondition: message must be a string of log message
    :precondition: log ETL process with the accepted message and timestamp
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


def etl_process_main(url):
    """Execute the etl process"""

    # Extract
    log("ETL Job Started")
    log("Extract phase Started")
    extracted_data = extract_from_web_data(url)
    log("Extract phase Ended")

    # Transform
    log("Transform phase Started")
    transformed_data = transform(extracted_data)
    log("Transform phase Ended")

    # Load
    log("Load phase Started")
    import_data_into_dynamodb(transformed_data)
    log("Load phase Ended")

    log("ETL Job Ended")


def main():
    """Execute the program."""
    etl_process_main(URL)


if __name__ == "__main__":
    main()
