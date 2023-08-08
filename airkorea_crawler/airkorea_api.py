import requests
import os
from requests import Response

def request_airkorea_api(station_name: str,page_no: int,data_term: str = "MONTH") -> Response :
    response = requests.get(
        f"{os.environ.get('AIRKOERA_API_URL')}/getMsrstnAcctoRltmMesureDnsty" # 확장성 위해
        f"?serviceKey={os.environ.get('AIRKOERA_API_URL')}"
        f"&returnType=json"
        f"&numOfRows=100"
        f"&pageNo={page_no}"
        f"&stationName={station_name}"
        f"&dataTerm={data_term}"
    )
    return response