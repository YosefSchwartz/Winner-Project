import time
import traceback

import pymysql
import requests
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
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
db_table_name = os.getenv("DB_TABLE_NAME_RES")

try:
    db_connection = pymysql.connect(host=db_hostname, user=db_username, password=db_password, database=db_database_name)
    db_cursor = db_connection.cursor()

    # Perform HTTP request to GetCMobileHashes API
    results_url = 'https://www.winner.co.il/api/v2/publicapi/GetResults'
    results_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'deviceid': '50dd938f151692ef21448500f9d2b5a3',
        'requestid': '88df46aae697b62054ba2e08bdfb5db0'
    }

    hashes_attempts = 0
    hashes_max_attempts = 3
    today = datetime.today()
    start = today - timedelta(days=2)
    end = today - timedelta(days=1)

    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')

    while hashes_attempts < hashes_max_attempts:
        results_response = requests.post(results_url, headers=results_headers, json={"startDate":start_str+"T00:00:00+02:00","endDate":end_str+"T00:00:00+02:00","sports":[240],"leagues":[]})

        if results_response.status_code < 300:
            break
        insert_log_record(datetime.now(), 'warning', 'GetResults',
                          'Failed to get results - Status Code: ' + str(results_response.status_code))
        hashes_attempts += 1
        time.sleep(hashes_attempts * 5)
        insert_log_record(datetime.now(), 'info', 'GetResults',
                          'Sleeping for ' + str(hashes_attempts * 5) + ' seconds')

    if results_response.status_code >= 300:
        insert_log_record(datetime.now(), 'error', 'GetResults',
                          'Failed to get results - Status Code: ' + str(results_response.status_code))
        raise Exception('Failed to get the results')
    else:
        insert_log_record(datetime.now(), 'info', 'GetResults', 'Results retrieved successfully.')


    # Manipulate the data
    data = results_response.json()
    data = data['results']['events']

    # Create the table
    final = []

    for idx, row in enumerate(data):
        new_row = {}
        new_row["event_id"] = int(row['eventid'])
        new_row["league_id"] = row['leagueid']
        new_row["match_datetime"] = row['date'] + " " + row['time']
        new_row["home_team"] = row['teamA']
        if 'scoreA' in row:
            new_row["home_score"] = int(row['scoreA'])
        new_row["away_team"] = row['teamB']
        if 'scoreB' in row:
            new_row["away_score"] = int(row['scoreB'])
        market = list(filter(lambda market : market['title'] == '‮1X2‬ תוצאת סיום (ללא הארכות)',row['markets']))
        if(len(market) > 0):
            new_row["market_result"] = market[0]['marketResults'][0]

        final.append(new_row)

    insert_log_record(datetime.now(), 'info', 'ResultsManipulation', 'Results manipulation completed successfully.')

    # Write to the database
    new_df = pd.DataFrame.from_records(final)

    engine = create_engine(
        'mysql+pymysql://' + db_username + ':' + db_password + '@' + db_hostname + '/' + db_database_name)
    conn = engine.connect()
    conn.execute(text("TRUNCATE TABLE winner_v2.events_result_tmp"))
    insert_log_record(datetime.now(), 'info', 'Truncate events_result_tmp', 'events_result_tmp truncated successfully.')
    new_df.to_sql(db_table_name, engine, if_exists='append', index=False)
    insert_log_record(datetime.now(), 'info', 'ResultsWriting', 'Results written to the database successfully.')

except Exception as exc:
    insert_log_record(datetime.now(), 'error', 'General error', f'Exception details: {exc} \nTraceback: {traceback.format_exc()}')
finally:
    if os.getenv("ENVIRONMENT") == "prod":
        close_vpn_connection(settings)
