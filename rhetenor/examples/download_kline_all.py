import os
import sys
import json
import time
import datetime
import concurrent.futures
import threading

# Add src to path to import rhetenor
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from rhetenor.data import download_kospi_master, HantooClient, DataLoader

# Configuration
TMP_DIR = "tmp"
BUCKET = "rhetenor"
PREFIX = "hantoo-stock-kline-1m"
AWS_CONFIG = "auth/aws_rhetenor.yaml"
HANTOO_CONFIG = "auth/hantoo.yaml"
HANTOO_TOKEN = "auth/hantoo_token.yaml"

def load_yaml(path):
    import yaml
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def download_data(instruments):
    h_conf = load_yaml(HANTOO_CONFIG)
    client = HantooClient(
        app_key=h_conf.get('my_app'),
        app_secret=h_conf.get('my_sec'),
        account_no=h_conf.get('my_acct_stock'),
        token_path=HANTOO_TOKEN
    )

    os.makedirs(TMP_DIR, exist_ok=True)
    
    # Rate limiter setup
    request_count = 0
    lock = threading.Lock()
    
    def rate_limit():
        nonlocal request_count
        with lock:
            request_count += 1
            if request_count % 10 == 0:
                time.sleep(1.0)
            else:
                time.sleep(0.1)

    print(f"Downloading data for {len(instruments)} instruments...")
    
    # Current date/time for query
    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S") # Current time, asking for recent data
    
    for i, symbol in enumerate(instruments):
        path = os.path.join(TMP_DIR, f"{symbol}.json")
        if os.path.exists(path):
            continue
            
        print(f"[{i+1}/{len(instruments)}] Fetching {symbol}...")
        
        all_data = []
        tr_cont = ""
        
        while True:
            rate_limit()
            
            headers, res = client.inquire_time_dailychartprice(
                symbol, date_str, time_str, period_code="N", tr_cont=tr_cont
            )
            
            if not res:
                break
                
            output2 = res.get('output2', [])
            if output2:
                all_data.extend(output2)
            
            # Check for continuation
            # Hantoo API usually returns tr_cont in header: 'tr_cont': 'D' or 'M' (Next exists) vs 'E' (End)?
            # Or simplified: if tr_cont header is 'M' or 'F', there is more data.
            # Let's check the header key usually used.
            # According to common Hantoo docs, 'tr_cont' in header indicates continuity.
            # 'M' or nothing usually. 
            # If the response header has 'tr_cont' == 'M' or 'F', we should request again with same tr_cont.
            # Wait, usually for NEXT page, we send tr_cont='N' (Next)? Or passing the 'M' back?
            # The standard behavior: 
            # 1st request: tr_cont=""
            # Response: tr_cont="M" (More data)
            # 2nd request: tr_cont="N" (Next data)
            
            resp_tr_cont = headers.get('tr_cont', '').strip().upper()
            if resp_tr_cont in ['F', 'M']:
                tr_cont = "N" # Request next page
            else:
                break
                
            # Safety break for huge data if unexpected
            if len(all_data) > 100000: 
                break
        
        # Save aggregated data
        # Mimic structure if multiple pages, or just save list of records?
        # User requirement: "save it to tmp/{symbol}.json"
        # We can save just the output2 list or wrapped. 
        # Making it consistent with reshape step, let's save list of `output2` items.
        with open(path, 'w') as f:
            json.dump({"output2": all_data}, f)
            
def reshape_and_process():
    print("Reshaping data...")
    # Buffer: timestamp -> { open: {}, high: {}, ... }
    data_buffer = {}
    
    files = [f for f in os.listdir(TMP_DIR) if f.endswith('.json')]
    for fname in files:
        symbol = fname.split('.')[0]
        with open(os.path.join(TMP_DIR, fname), 'r') as f:
            try:
                content = json.load(f)
                records = content.get('output2', [])
            except json.JSONDecodeError:
                continue
                
        for r in records:
            # Format: 'stck_bsop_date': '20240123', 'stck_cntg_hour': '130000'
            try:
                ts_str = f"{r['stck_bsop_date']}_{r['stck_cntg_hour']}"
                dt = datetime.datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                
                # We need YYYYMMDD key for buffering by day if we upload separate files?
                # User request: "upload it to S3 as YYYYMMDD_235959.jsonl.zstd"
                # So we eventually group by DAY.
                
                if dt not in data_buffer:
                    data_buffer[dt] = {
                        "timestamp": dt.strftime("%Y-%m-%d_%H:%M:%S"),
                        "open": {}, "high": {}, "low": {}, "close": {}, "volume": {}
                    }
                
                data_buffer[dt]["open"][symbol] = int(r.get('stck_oprc', 0))
                data_buffer[dt]["high"][symbol] = int(r.get('stck_hgpr', 0))
                data_buffer[dt]["low"][symbol] = int(r.get('stck_lwpr', 0))
                data_buffer[dt]["close"][symbol] = int(r.get('stck_prpr', 0))
                data_buffer[dt]["volume"][symbol] = int(r.get('cntg_vol', 0))
                
            except (ValueError, KeyError):
                continue
                
    return data_buffer

def upload_to_s3(data_buffer):
    print("Checking S3 and uploading...")
    loader = DataLoader(BUCKET, PREFIX, AWS_CONFIG)
    s3 = loader.s3
    
    # Identify unique days
    # Group by YYYYMMDD
    daily_groups = {}
    for dt, entry in data_buffer.items():
        day_key = dt.strftime("%Y%m%d")
        if day_key not in daily_groups:
            daily_groups[day_key] = []
        daily_groups[day_key].append((dt, entry))
        
    # Check what exists
    # Upload based on what we have, but avoid overwriting if not needed.
    # TODO : Usually we overwrite to ensure latest completeness, so add an option to overwrite or not 
    
    import zstandard as zstd
    import io
    
    for day_key, entries in daily_groups.items():
        # Target filename: YYYYMMDD_235959.jsonl.zstd
        # Note: If we really want "backfill", we might check if file exists. 
        filename = f"{day_key}_235959.jsonl.zstd"
        key = f"{PREFIX}/{filename}"
        
        # Sort entries by time
        entries.sort(key=lambda x: x[0])
        
        # Prepare content
        buffer = io.BytesIO()
        cctx = zstd.ZstdCompressor()
        with cctx.stream_writer(buffer) as writer:
            text_writer = io.TextIOWrapper(writer, encoding='utf-8')
            for dt, entry in entries:
                # Using default str for timestamp serialization if needed, but entry has string timestamp
                text_writer.write(json.dumps(entry) + "\n")
            text_writer.flush()
            
        print(f"Uploading {key} ({len(entries)} records)...")
        # buffer might be closed by stream_writer, but getvalue() should work 
        # or we check if stream_writer closes it. 
        # Actually, python-zstandard stream_writer might close the underlying stream by default?
        # Let's try getvalue().
        s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


def main():
    # 1. Download Master
    print("loading master...")
    universe = download_kospi_master(verbose=True)
    instruments = [k for k,v in universe.items() if v["kospi50"]=='Y']
    print(f"Found {len(instruments)} instruments.")
    
    # 2. Download from Hantoo
    download_data(instruments)
    
    # 3. Reshape
    data_buffer = reshape_and_process()
    
    # 4 & 5. Upload
    upload_to_s3(data_buffer)

if __name__ == "__main__":
    main()
