import os
import dotenv
from airkorea_crawler import airkorea_api
if __name__ == "__main__":
    dotenv.load_dotenv()

    response = airkorea_api.request_airkorea_api(
        station_name="마포구",
        page_no=1,
        data_term="MONTH"
    )
    print(response)