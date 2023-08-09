import datetime
import re

def get_hour(dt: str) -> str:
    pattern = "\d{4}-\d{2}-\d{2} (\d{2}):\d{2}"
    result = re.findall(pattern,dt)
    return result[-1]

def convert_dt(dt: str) -> datetime.datetime:
    dt_format = "%Y-%m-%d %H:%M"
    pattern = "(\d{4}-\d{2}-\d{2}) \d{2}(:\d{2})"
    prev_dt = re.sub(pattern,r"\1 23\2",dt)
    result =  datetime.datetime.strptime(prev_dt,dt_format) \
        + datetime.timedelta(hours=1)
    return result

def get_datalake_bucket_name(layer,company,region,account,env):
    return f"{company}-{layer}-{region}-{account}-{env}"