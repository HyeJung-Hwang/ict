import os
import dotenv
import json
import pprint
from airkorea_crawler import airkorea_api


def run_extract():
    # ETL 중 Extract
    dotenv.load_dotenv()

    response = airkorea_api.request_airkorea_api(
        station_name="마포구", page_no=1, data_term="MONTH"
    )
    print(response)
    if response.status_code != 200:
        return json.dumps(response)
    parsed_airdata = airkorea_api.parse_airdata(response.content)
    print(parsed_airdata)


if __name__ == "__main__":
    run_extract()
