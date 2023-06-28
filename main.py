from datetime import datetime
import os
import pandas as pd  # pip install pandas
from nordvpn_connect import initialize_vpn, rotate_VPN, close_vpn_connection
import requests as re  # pip install requests
from dotenv import load_dotenv  # pip install python-dotenv
from sqlalchemy import create_engine

load_dotenv()
check = os.getenv("NORDVPN_USERNAME")
print(check)
if os.getenv("ENVIRONMENT") == "prod":
    print("prod")
    settings = initialize_vpn("Israel", '2PfHqdoH9frPgQzud8ZFR9wV', 'c8C98ZnDGX3dMCrdHoHGBFyD')  # starts nordvpn and stuff
    rotate_VPN(settings)  # actually connect to server
try:
    s = re.session()
    print("1 - Before 1st http request")
    checksum_response = s.get('https://api.winner.co.il/v2/publicapi/GetCMobileHashes',
                              headers={
                                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
                                  'deviceid': '50dd938f151692ef21448500f9d2b5a3',
                                  'requestid': '88df46aae697b62054ba2e08bdfb5db0'
                              })

    print("2 - After 1st request, res: ", checksum_response)
    checksum_obj = checksum_response.json()
    print("3 - Before 2st http request")
    data = s.get('https://api.winner.co.il/v2/publicapi/GetCMobileLine?lineChecksum=' + checksum_obj['lineChecksum'],
                 headers={
                     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'

                 })
    print("4 - After 2st request, res: ", data)
    data = data.json()
    data = data['markets']
    # filter the relevant data
    # data = list(filter(lambda row: "תוצאת סיום" in row['mp'] and "1X2" in row['mp'], data))
    data = list(filter(lambda row: row['sId'] == 240, data))
    data = list(filter(lambda row: len(row['outcomes']) == 3, data))
    print("5 - After filters")
    final = []
    # Get the current date and time
    now = datetime.now()
    # Format the date and time as a string
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

    print("6 - After build the result array")
    # Create a new DataFrame
    new_df = pd.DataFrame.from_records(final)

    try:
        engine = create_engine(
            'mysql+pymysql://' + os.getenv("DB_USERNAME") + ':' + os.getenv("DB_PASSWORD") + '@' + os.getenv(
                "DB_HOSTNAME") + '/' + os.getenv("DB_DATABASE_NAME"))
        new_df.to_sql(os.getenv("DB_TABLE_NAME"), engine, if_exists='append', index=False)
    except Exception as exc:
        log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + "_log.txt"

        # Open the log file in write mode
        with open(log_filename, "w") as file:
            # Write the exception details to the file
            file.write("An exception occurred: {}\n".format(str(exc)))
            file.write("Traceback:\n")
            import traceback

            traceback.print_exc(file=file)

except Exception as e:
    print(e)
finally:
    if os.getenv("ENVIRONMENT") == "prod":
        close_vpn_connection(settings)
