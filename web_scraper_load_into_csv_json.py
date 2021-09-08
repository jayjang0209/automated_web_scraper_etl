"""
Web scraper - Data ETL (Extract, Transform, Load)
Export the extracted data into json or csv file
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
from dateutil.parser import parse
from datetime import datetime

URL = "https://www.canada.ca/en/immigration-refugees-citizenship/corporate/mandate/policies-operational-instructions-agreements/ministerial-instructions/express-entry-rounds.html"
DATA_COLUMNS = ["date", "program", "invitations", "lowest_crs"]
FILE_NAME = "CRS_draw_score_history"


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
            date = col[1].text  # change date format - Transformation
            program = col[2].text
            invitations = col[3].text  # Remove 1000's separator - Transformation
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


def load_data_into_json(data, file_name):
    """Load the extracted data into a json file.

    :param data: data must be a pandas dataFrame object
    :param file_name: string
    :precondition: data must be a pandas dataFrame object
    :precondition: file_name must be a string of the target file name
    :postcondition: exports data in a json format with accepted file name
    """
    data.to_json(file_name + ".json", orient="records")


def load_data_into_csv(data, file_name):
    """Load the extracted data into a csv file.

    :param data: pandas dataFrame object
    :param file_name: string
    :precondition: data must be a pandas dataFrame object
    :precondition: file_name must be a string of the target file name
    :postcondition: exports data in a csv format with accepted file name
    """
    data.to_csv(file_name + ".csv")


def log(message):
    """Log ETL process.

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
    load_data_into_json(transformed_data, FILE_NAME)
    load_data_into_csv(transformed_data, FILE_NAME)
    log("Load phase Ended")

    log("ETL Job Ended")


def main():
    """Execute the program."""
    etl_process_main(URL)


if __name__ == "__main__":
    main()
