import time
import traceback

import pymysql
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv  # pip install python-dotenv
from nordvpn_connect import initialize_vpn, rotate_VPN, close_vpn_connection

load_dotenv()


# Function to insert a log record into the database
def insert_log_record(timestamp, level, title, description):
    insert_query = '''
        INSERT INTO `logs` (timestamp, level, title, description) VALUES (%s, %s, %s, %s)
    '''

    db_cursor.execute(insert_query, (timestamp, level, title, description))
    db_connection.commit()
    time.sleep(1)


if os.getenv("ENVIRONMENT") == "prod":
    settings = initialize_vpn("Israel", '2PfHqdoH9frPgQzud8ZFR9wV', 'c8C98ZnDGX3dMCrdHoHGBFyD')  # starts nordvpn and stuff
    rotate_VPN(settings)  # actually connect to server

# Configure database connection
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_hostname = os.getenv("DB_HOSTNAME")
db_database_name = os.getenv("DB_DATABASE_NAME")
db_table_name = os.getenv("DB_TABLE_NAME")

try:
    db_connection = pymysql.connect(host=db_hostname, user=db_username, password=db_password, database=db_database_name)
    db_cursor = db_connection.cursor()

    # Perform HTTP request to GetCMobileHashes API
    hashes_url = 'https://api.winner.co.il/v2/publicapi/GetCMobileHashes'
    hashes_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'deviceid': '50dd938f151692ef21448500f9d2b5a3',
        'requestid': '88df46aae697b62054ba2e08bdfb5db0'
    }

    hashes_attempts = 0
    hashes_max_attempts = 3

    while hashes_attempts < hashes_max_attempts:
        hashes_response = requests.get(hashes_url, headers=hashes_headers)
        if hashes_response.status_code < 300:
            break
        insert_log_record(datetime.now(), 'warning', 'GetCMobileHashes',
                          'Failed to get hashes - Status Code: ' + str(hashes_response.status_code))
        hashes_attempts += 1
        time.sleep(hashes_attempts * 5)
        insert_log_record(datetime.now(), 'info', 'GetCMobileHashes',
                          'Sleeping for ' + str(hashes_attempts * 5) + ' seconds')

    if hashes_response.status_code >= 300:
        insert_log_record(datetime.now(), 'error', 'GetCMobileHashes',
                          'Failed to get hashes - Status Code: ' + str(hashes_response.status_code))
        raise Exception('Failed to retrieve the first HTTP call')
    else:
        insert_log_record(datetime.now(), 'info', 'GetCMobileHashes', 'Hashes retrieved successfully.')

    # Perform HTTP request to GetCMobileLine API
    checksum = hashes_response.json()['lineChecksum']
    line_url = 'https://api.winner.co.il/v2/publicapi/GetCMobileLine?lineChecksum=' + checksum
    line_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    }

    line_attempts = 0
    line_max_attempts = 3

    while line_attempts < line_max_attempts:
        line_response = requests.get(line_url, headers=line_headers)
        if line_response.status_code < 300:
            break
        insert_log_record(datetime.now(), 'warning', 'GetCMobileLine',
                          'Failed to get line data - Status Code: ' + str(line_response.status_code))
        line_attempts += 1
        time.sleep(line_attempts * 5)
        insert_log_record(datetime.now(), 'info', 'GetCMobileLine',
                          'Sleeping for ' + str(line_attempts * 5) + ' seconds')


    if line_response.status_code >= 300:
        insert_log_record(datetime.now(), 'error', 'GetCMobileLine',
                          'Failed to get line data - Status Code: ' + str(line_response.status_code))
        raise Exception('Failed to retrieve the second HTTP call')
    else:
        insert_log_record(datetime.now(), 'info', 'GetCMobileLine', 'Line data retrieved successfully.')

    # Manipulate the data
    data = line_response.json()
    data = data['markets']
    data = list(filter(lambda row: row['sId'] == 240, data))
    data = list(filter(lambda row: len(row['outcomes']) == 3, data))

    # Create the table
    final = []
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")

    for idx, row in enumerate(data):
        row['timestamp'] = date_time
        row['home_desc'] = row['outcomes'][0]['desc']
        row['home_rate'] = row['outcomes'][0]['price']
        row['draw_desc'] = row['outcomes'][1]['desc']
        row['draw_rate'] = row['outcomes'][1]['price']
        row['away_desc'] = row['outcomes'][2]['desc']
        row['away_rate'] = row['outcomes'][2]['price']
        del (row['outcomes'])
        del (row['players'])
        final.append(row)

    insert_log_record(datetime.now(), 'info', 'DataManipulation', 'Data manipulation completed successfully.')

    # Write to the database
    new_df = pd.DataFrame.from_records(final)

    engine = create_engine(
        'mysql+pymysql://' + db_username + ':' + db_password + '@' + db_hostname + '/' + db_database_name)
    new_df.to_sql(db_table_name, engine, if_exists='append', index=False)
    insert_log_record(datetime.now(), 'info', 'DataWriting', 'Data written to the database successfully.')

except Exception as exc:
    insert_log_record(datetime.now(), 'error', 'General error', f'Exception details: {exc} \nTraceback: {traceback.format_exc()}')
finally:
    if os.getenv("ENVIRONMENT") == "prod":
        close_vpn_connection(settings)
