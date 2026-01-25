from datetime import datetime, timedelta
import pandas as pd 
from rhetenor import data

s3 = data.S3KlineWrapper(exchange_code="UN")
s3.load(datetime_from=datetime.now()-timedelta(days=10), datetime_to=datetime.now())
df = pd.DataFrame({k: pd.DataFrame(v["data"], index=v["fields"]).stack() for k,v in s3.loaded_data_map.items()}).stack()
df.sort_index()