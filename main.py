from datetime import datetime
import os
import time
import pandas as pd  # pip install pandas
from nordvpn_connect import initialize_vpn, rotate_VPN, close_vpn_connection
import requests as re  # pip install requests
from dotenv import load_dotenv  # pip install python-dotenv
import mysql.connector
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

load_dotenv()
current_time = datetime.now()
formatted_time = current_time.strftime('%Y-%m-%d %H:%M')

mydb = mysql.connector.connect(
    host=os.getenv("DB_HOSTNAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE_NAME")
)
cursor = mydb.cursor()

insert_log_row = ("INSERT INTO logs "
                  "(title, description, level) "
                  "VALUES (%(title)s, %(description)s, %(level)s)")


def write_to_log(log_data):
    """
    Get object that MUST contain
    title - VARCHAR(150)
    description - TEXT - up to 65,535 characters
    level - INT - 1 (normal) to 5 (severe)

    :param log_data:
    :return:
    """
    cursor.execute(insert_log_row, log_data)
    mydb.commit()


def terminate_script(exit_status):
    cursor.close()
    mydb.close()
    if os.getenv("ENVIRONMENT") == "prod":
        close_vpn_connection(settings)
    exit(exit_status)


COUNTER_ATTEMPTS = 0
MAX_ATTEMPTS = 0

log_data = {
    'title': "Start Script",
    'description': f"Winner scraper -- Start",
    'level': 1
}
write_to_log(log_data)

# Connect VPN
if os.getenv("ENVIRONMENT") == "prod":
    settings = initialize_vpn("Israel", '2PfHqdoH9frPgQzud8ZFR9wV', 'c8C98ZnDGX3dMCrdHoHGBFyD')  # starts nordvpn and stuff
    rotate_VPN(settings)  # actually connect to server
try:
    s = re.session()
    attempt = 0
    while attempt < 3:
        try:
            log_data = {
                'title': "First call",
                'description': f"Execute the first call #{attempt}",
                'level': 1
            }
            write_to_log(log_data)

            checksum_call = s.get('https://api.winner.co.il/v2/publicapi/GetCMobileHashes',
                                  headers={
                                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
                                      'deviceid': '50dd938f151692ef21448500f9d2b5a3',
                                      'requestid': '88df46aae697b62054ba2e08bdfb5db0'
                                  })
            if (checksum_call.status_code < 300):
                break
            else:
                log_data = {
                    'title': "First call",
                    'description': f"Got status code {checksum_call.status_code}",
                    'level': 2
                }
                write_to_log(log_data)
                attempt+=1
            time.sleep(3)
        except Exception as e:
            log_data = {
                'title': "First call",
                'description': f"Execution of the first call failed",
                'level': 3
            }
            write_to_log(log_data)

            log_data = {
                'title': "First call",
                'description': f"Error: {e}",
                'level': 3
            }
            write_to_log(log_data)
            terminate_script(1)

    if attempt == 3:
        raise Exception('Failed to make the first call')
    checksum_obj = checksum_call.json()

    attempt = 0
    while attempt < 3:
        try:
            log_data = {
                'title': "Second call",
                'description': f"Execute the second call #{attempt}",
                'level': 1
            }
            write_to_log(log_data)

            data_call = s.get(
                'https://api.winner.co.il/v2/publicapi/GetCMobileLine?lineChecksum=' + checksum_obj['lineChecksum'],
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
                })
            if (data_call.status_code < 300):
                break
            else:
                log_data = {
                    'title': "Second call",
                    'description': f"Got status code {data_call.status_code}",
                    'level': 2
                }
                write_to_log(log_data)
                attempt += 1
            time.sleep(3)

        except Exception as e:
            log_data = {
                'title': "Second call",
                'description': f"Execution of the second call failed",
                'level': 3
            }
            write_to_log(log_data)

            log_data = {
                'title': "Second call",
                'description': f"Error: {e}",
                'level': 3
            }
            write_to_log(log_data)
            terminate_script(1)

    if attempt == 3:
        raise Exception('Failed to make the second call')

    data = data_call.json()
    data = data['markets']
    # filter the relevant data

    data = list(filter(lambda row: row['sId'] == 240, data))
    data = list(filter(lambda row: len(row['outcomes']) == 3, data))

    final = []
    for idx, row in enumerate(data):
        row['timestamp'] = formatted_time
        row['home_desc'] = row['outcomes'][0]['desc']
        row['home_rate'] = row['outcomes'][0]['price']
        row['draw_desc'] = row['outcomes'][1]['desc']
        row['draw_rate'] = row['outcomes'][1]['price']
        row['away_desc'] = row['outcomes'][2]['desc']
        row['away_rate'] = row['outcomes'][2]['price']
        del (row['outcomes'])
        del (row['players'])
        final.append(row)

    # Create a new DataFrame
    new_df = pd.DataFrame.from_records(final)

    attempt = 0
    while attempt < 3:
        try:
            log_data = {
                'title': "Write data to DB",
                'description': f"Try to write the whole data to DB #{attempt}",
                'level': 1
            }
            write_to_log(log_data)

            engine = create_engine(
                    'mysql+pymysql://' + os.getenv("DB_USERNAME") + ':' + os.getenv("DB_PASSWORD") + '@' + os.getenv(
                        "DB_HOSTNAME") + '/' + os.getenv("DB_DATABASE_NAME"))
            new_df.to_sql(os.getenv("DB_TABLE_NAME"), engine, if_exists='append', index=False)
            break
        except Exception as e:
            log_data = {
                'title': "Write data to DB",
                'description': f"Failed to write the data to DB",
                'level': 3
            }
            write_to_log(log_data)
            log_data = {
                'title': "Write data to DB",
                'description': f"Error: {e}",
                'level': 3
            }
            write_to_log(log_data)
            attempt+=1

    if attempt == 3:
        raise Exception('Failed to write the data to DB')

    terminate_script(0)
except Exception as e:
    log_data = {
        'title': "Got an exception",
        'description': f"Error: {e}",
        'level': 3
    }
    write_to_log(log_data)
    terminate_script(1)

