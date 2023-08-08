import os
import dotenv
import json
import pprint
from airkorea_crawler import airkorea_api
if __name__ == "__main__":
    dotenv.load_dotenv()

    response = airkorea_api.request_airkorea_api(
        station_name="마포구",
        page_no=1,
        data_term="MONTH"
    )
    print(response)
    if response.status_code != 200:
        #  json.dumps(response)
        exit(0) # early return
    else:
        parsed_airdata = airkorea_api.parse_airdata(response.content)
    print(parsed_airdata)