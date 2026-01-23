
import os
import yaml
import boto3
import zstandard as zstd
import json
import io
from datetime import datetime
from typing import Iterator, Dict, Any, Optional, List, Union
import requests
import zipfile
import threading
import time


class DataLoader:
    def __init__(self, bucket: str, prefix: str, auth_config_path: str = "auth/aws_rhetenor.yaml", region: Optional[str] = None):
        """
        Initialize the DataLoader.

        Args:
            bucket: The S3 bucket name.
            prefix: The S3 key prefix where logs are stored.
            auth_config_path: Path to the AWS credentials YAML file.
            region: AWs region (overrides config if provided).
        """
        self.bucket = bucket
        self.prefix = prefix
        self.auth_config = self._load_credentials(auth_config_path)
        
        region_name = region or self.auth_config.get('region')
        
        self.s3 = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=self.auth_config.get('access_key_id'),
            aws_secret_access_key=self.auth_config.get('secret_access_key')
        )

    def _load_credentials(self, path: str) -> Dict[str, str]:
        """Load AWS credentials from a YAML file."""
        if not os.path.exists(path):
            raise FileNotFoundError(f"AWS config file not found at {path}")
        
        with open(path, 'r') as f:
            try:
                config = yaml.safe_load(f)
                return config
            except yaml.YAMLError as e:
                raise ValueError(f"Failed to parse AWS config YAML: {e}")

    def list_objects(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Iterator[str]:
        """
        List S3 objects in the bucket with the given prefix, optionally filtered by date.
        Assumes filenames contain timestamps in the format: {prefix}/{timestamp}_...
        Timestamp format from Rust logger: %Y%m%d_%H%M%S
        """
        paginator = self.s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

        for page in page_iterator:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                # Filename format expected: key_prefix/YYYYMMDD_HHMMSS_UUID.jsonl.zstd
                # We need to extract the part after the prefix and try to parse the timestamp
                
                # Check if it matches expected extension
                if not key.endswith('.jsonl.zstd'):
                    continue

                if start_date or end_date:
                    try:
                        # Extract filename part
                        filename = os.path.basename(key)
                        # Split by '_' to get date and time parts. 
                        # Format: YYYYMMDD_HHMMSS_uuid.jsonl.zstd
                        parts = filename.split('_')
                        if len(parts) >= 2:
                            date_str = parts[0]
                            time_str = parts[1]
                            timestamp_str = f"{date_str}_{time_str}"
                            
                            file_dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                            
                            if start_date and file_dt < start_date:
                                continue
                            if end_date and file_dt > end_date:
                                continue
                    except (ValueError, IndexError):
                        # If parsing fails, maybe include it or warn? 
                        # For now, let's skip files that don't match the format if we are filtering.
                        # If we strictly enforce format, skip.
                        pass
                
                yield key

    def download_and_parse(self, key: str) -> Iterator[Dict[str, Any]]:
        """
        Download a specific object, decompress it, and parse JSONL lines.
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = response['Body']
            
            dctx = zstd.ZstdDecompressor()
            
            # Streaming decompression
            with dctx.stream_reader(body) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line in text_stream:
                    if line.strip():
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in file {key}: {e}")
                            
        except Exception as e:
            print(f"Error processing file {key}: {e}")
            raise


    def load(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator that yields parsed log entries from all matching files.
        """
        for key in self.list_objects(start_date, end_date):
            yield from self.download_and_parse(key)

def download_kospi_master(verbose: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Downloads and parses the KOSPI master file in-memory.
    Returns a dictionary mapping short code to information dictionary.
    """
    url = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
    
    # Field specifications for the second part of the row (last 228 bytes)
    # field_specs contains widths of each field
    field_specs = [
        2, 1, 4, 4, 4,              # group_code .. industry_small
        1, 1, 1, 1, 1,              # manufacturing .. kospi100
        1, 1, 1, 1, 1,              # kospi50 .. krx100
        1, 1, 1, 1, 1,              # krx_auto .. spac
        1, 1, 1, 1, 1,              # krx_energy_chemical .. krx_construction
        1, 1, 1, 1, 1,              # non1 .. krx_sector_transport
        1, 9, 5, 5, 1,              # sri .. trading_halt
        1, 1, 2, 1, 1,              # liquidation .. dishonest_disclosure
        1, 2, 2, 2, 3,              # bypass_listing .. margin_rate
        1, 3, 12, 12, 8,            # credit_available .. listing_date
        15, 21, 2, 7, 1,            # listed_shares .. preferred_stock
        1, 1, 1, 1, 9,              # short_sale_overheat .. sales
        9, 9, 5, 9, 8,              # operating_profit .. base_year_month
        9, 3, 1, 1, 1               # market_cap .. securities_lending_available
    ]

    part2_columns = [
        'group_code', 'market_cap_scale', 'industry_large', 'industry_medium', 'industry_small',
        'manufacturing', 'low_liquidity', 'governance_index_stock', 'kospi200_sector_industry', 'kospi100',
        'kospi50', 'krx', 'etp', 'elw_issuance', 'krx100',
        'krx_auto', 'krx_semiconductor', 'krx_bio', 'krx_bank', 'spac',
        'krx_energy_chemical', 'krx_steel', 'short_term_overheat', 'krx_media_telecom', 'krx_construction',
        'non1', 'krx_security', 'krx_ship', 'krx_sector_insurance', 'krx_sector_transport',
        'sri', 'base_price', 'trading_unit', 'after_hours_unit', 'trading_halt',
        'liquidation', 'management_stock', 'market_warning', 'warning_forecast', 'dishonest_disclosure',
        'bypass_listing', 'lock_division', 'par_value_change', 'capital_increase', 'margin_rate',
        'credit_available', 'credit_period', 'prev_day_volume', 'par_value', 'listing_date',
        'listed_shares', 'capital', 'settlement_month', 'public_offering_price', 'preferred_stock',
        'short_sale_overheat', 'unusual_rise', 'krx300', 'kospi', 'sales',
        'operating_profit', 'ordinary_profit', 'net_income', 'roe', 'base_year_month',
        'market_cap', 'group_company_code', 'company_credit_limit_exceed', 'collateral_loan_available', 'securities_lending_available'
    ]

    try:
        if verbose:
            print(f"Downloading KOSPI master file from {url}...")
        
        # Download in-memory
        response = requests.get(url, verify=False) # Verify=False as per example code usage (ssl unsafe context)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Expected file inside zip: kospi_code.mst
            if 'kospi_code.mst' not in zf.namelist():
                raise FileNotFoundError("kospi_code.mst not found in the downloaded zip file.")
            
            with zf.open('kospi_code.mst') as f:
                # Need to decode cp949
                content = f.read().decode('cp949')
                
        # Parse the content
        master_dict = {}
        for line in content.splitlines():
            # Based on logic from master_file.py:
            # rf1 = row[0:len(row) - 228]
            # rf2 = row[-228:]
            
            if len(line) <= 228:
                continue
                
            part1 = line[:-228]
            part2 = line[-228:]
            
            # Part 1 Parsing
            # rf1_1 = rf1[0:9].rstrip() -> Short Code
            # rf1_2 = rf1[9:21].rstrip() -> Standard Code
            # rf1_3 = rf1[21:].strip()   -> Korean Name
            
            if len(part1) < 21:
                continue
                
            short_code = part1[0:9].strip()
            standard_code = part1[9:21].strip()
            korean_name = part1[21:].strip()
            
            entry = {
                "standard_code": standard_code,
                "korean_name": korean_name
            }
            
            # Part 2 Parsing
            # Slice part2 according to field_specs
            curr_idx = 0
            for i, width in enumerate(field_specs):
                if i >= len(part2_columns):
                    break
                
                col_name = part2_columns[i]
                if curr_idx + width <= len(part2):
                    val = part2[curr_idx : curr_idx + width].strip()
                    entry[col_name] = val
                else:
                    entry[col_name] = ""
                    
                curr_idx += width
                
            master_dict[short_code] = entry
        
        if verbose:
            print(f"Parsed {len(master_dict)} entries from KOSPI master file.")
            
        return master_dict

    except Exception as e:
        print(f"Failed to download or parse KOSPI master file: {e}")
        raise

class HantooClient:
    """
    Client for interacting with Korea Investment Securities (Hantoo) API.
    Handles authentication, token management, and specific API calls.
    """
    BASE_URL_PROD = "https://openapi.koreainvestment.com:9443"
    
    def __init__(self, app_key: str, app_secret: str, account_no: str = "", mock: bool = False, token_path: str = "token.yaml"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.account_no = account_no
        self.mock = mock
        self.base_url = self.BASE_URL_PROD # Currently defaulting to PROD
        self.token_path = token_path
        self._access_token = None
        self._token_expiry = None
        
        self._load_token()
        
    def _load_token(self):
        """Load token from local file if valid, otherwise request new one."""
        if os.path.exists(self.token_path):
            with open(self.token_path, 'r') as f:
                try:
                    data = yaml.safe_load(f)
                    token = data.get('token')
                    expiry_str = data.get('valid-date') # Format: YYYY-mm-dd HH:MM:SS
                    
                    if token and expiry_str:
                        expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
                        if expiry_dt > datetime.now():
                            self._access_token = token
                            self._token_expiry = expiry_dt
                            return
                except Exception as e:
                    print(f"Failed to load token from {self.token_path}: {e}")
        
        # If we reach here, we need a new token
        self._issue_token()
        
    def _issue_token(self):
        """Issue a new access token and save it."""
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
        
        self._access_token = data['access_token']
        expiry_str = data['access_token_token_expired'] # Format: 2022-08-30 08:30:00
        self._token_expiry = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
        
        # Save to file
        os.makedirs(os.path.dirname(self.token_path), exist_ok=True)
        with open(self.token_path, 'w') as f:
            yaml.dump({
                'token': self._access_token,
                'valid-date': expiry_str
            }, f)
            
    def get_headers(self, tr_id: str, tr_cont: str = "") -> Dict[str, str]:
        """Generate headers for API requests."""
        if not self._access_token or (self._token_expiry and datetime.now() >= self._token_expiry):
            self._issue_token()
            
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "tr_cont": tr_cont,
            "custtype": "P" # Individual
        }

    def check_holiday(self, date_str: str) -> Dict[str, Any]:
        """
        Check if a date is a holiday.
        API: /uapi/domestic-stock/v1/quotations/chk-holiday
        TR_ID: CTCA0903R
        """
        path = "/uapi/domestic-stock/v1/quotations/chk-holiday"
        url = f"{self.base_url}{path}"
        tr_id = "CTCA0903R"
        
        headers = self.get_headers(tr_id)
        params = {
            "BASS_DT": date_str,
            "CTX_AREA_FK": "",
            "CTX_AREA_NK": ""
        }
        
        resp = requests.get(url, headers=headers, params=params)
        
        if resp.status_code != 200:
             # Try to print error but don't fail immediately, return empty
             print(f"Holiday check failed: {resp.text}")
             return {}
             
        data = resp.json()
        if data['rt_cd'] != '0':
            print(f"API Error in holiday check: {data.get('msg1')}")
            return {}
            
        # The output is a list of days.
        # We are usually asking for a specific day or getting a range.
        # The API returns a list in 'output'.
        return data

    def inquire_time_dailychartprice(self, symbol: str, date: str, time_hhmmss: str, period_code: str = "N", include_fake: str = "", tr_cont: str = "") -> tuple[Dict[str, str], Dict[str, Any]]:
        """
        Get minute (kline) data.
        API: /uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice
        TR_ID: FHKST03010230

        Returns:
            Tuple of (response_headers, response_json)
        """
        path = "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
        url = f"{self.base_url}{path}"
        tr_id = "FHKST03010230"
        
        headers = self.get_headers(tr_id, tr_cont=tr_cont)
        
        # Cond market div code: J (Stock), but required parameter.
        # The example code uses "J".
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_HOUR_1": time_hhmmss,
            "FID_INPUT_DATE_1": date,
            "FID_PW_DATA_INCU_YN": period_code, # N: No past data? Check docs/example. Actually param name is 'past data include yn'.
            "FID_FAKE_TICK_INCU_YN": include_fake
        }
        
        resp = requests.get(url, headers=headers, params=params)
        
        if resp.status_code != 200:
             print(f"Kline fetch failed: {resp.text}")
             return {}, {}
             
        data = resp.json()
        if data['rt_cd'] != '0':
            # Check for rate limit or other specific errors if needed
            print(f"API Error in kline fetch: {data.get('msg1')}")
            # Potentially handle rate limiting here
            
        return resp.headers, data


class HantooKlineLogger:
    def __init__(self, symbols: List[str], 
                 hantoo_config_path: str = "./auth/hantoo.yaml",
                 hantoo_token_path: str = "./auth/hantoo_token.yaml",
                 aws_config_path: str = "./auth/aws_rhetenor.yaml",
                 bucket: str = "rhetenor",
                 prefix: str = "hantoo-stock-kline-1m"):
        self.symbols = symbols
        self.bucket = bucket
        self.prefix = prefix
        self.aws_config_path = aws_config_path
        
        # Load Hantoo Credentials
        if not os.path.join(os.getcwd(), hantoo_config_path):
             # Try absolute?
             pass
        
        # Helper to safely load yaml
        def load_yaml(path):
            if not os.path.exists(path):
                raise FileNotFoundError(f"Config file not found: {path}")
            with open(path, 'r') as f:
                return yaml.safe_load(f)

        h_conf = load_yaml(hantoo_config_path)
            
        self.hantoo_client = HantooClient(
            app_key=h_conf.get('my_app'), # Support both keys just in case
            app_secret=h_conf.get('my_sec'),
            account_no=h_conf.get('my_acct_stock'),
            token_path=hantoo_token_path
        )
        
        # Initialize AWS DataLoader
        self.s3_loader = DataLoader(bucket, prefix, aws_config_path)
        self.s3_client = self.s3_loader.s3
        
        self.universe = {}
        self.holidays = []
        self.last_timestamp = None
        self.cache_buffer = {} # buffer for data to be uploaded

        self.init_data_flow()

    def init_data_flow(self):
        """
        Initialize data flow:
        1. Download cached data info (find last timestamp).
        2. Download KOSPI master.
        3. Check holidays.
        4. Fill gaps.
        """
        print("Initializing data flow...")
        
        # 1. Download Master
        self.universe = download_kospi_master(verbose=True)
        # Filter symbols if needed. User "instruments symbol list" is authoritative.
        # Ensure we don't request symbols invalid for Hantoo.
        valid_symbols = [s for s in self.symbols if s in self.universe]
        if len(valid_symbols) < len(self.symbols):
            print(f"Warning: {len(self.symbols) - len(valid_symbols)} symbols not found in KOSPI universe. Using {len(valid_symbols)} valid symbols.")
        # self.symbols = valid_symbols # Uncomment to enforce valid symbols only
        
        # 2. Find last timestamp in S3
        print("Checking S3 for last timestamp...")
        max_dt = None
        # This list_objects might take time if bucket is huge.
        # But we only need the "last" one. 
        # Since files are prefixed, we assume lexicographical order roughly correlates with time (YYYYMMDD_HHMMSS), 
        # but `list_objects` returns in key order usually.
        # So iterating all is safer.
        for key in self.s3_loader.list_objects():
            try:
                # Format: prefix/YYYYMMDD_HHMMSS.jsonl.zstd
                fname = os.path.basename(key)
                if not fname.endswith('.jsonl.zstd'): continue
                ts_str = fname.split('.')[0] 
                # Expect YYYYMMDD_HHMMSS
                dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                if max_dt is None or dt > max_dt:
                    max_dt = dt
            except ValueError:
                continue
                
        self.last_timestamp = max_dt
        print(f"Last cached timestamp: {self.last_timestamp}")
        
        # 3. Check Holidays
        today_str = datetime.now().strftime("%Y%m%d")
        holiday_info = self.hantoo_client.check_holiday(today_str)
        # We can store holidays if needed.
        
        # 4. Fill gaps
        if self.last_timestamp:
            start_fill = self.last_timestamp
        else:
            # Assume start of today 09:00:00
            now = datetime.now()
            start_fill = datetime(now.year, now.month, now.day, 9, 0, 0)
            
        self.fill_data(start_fill)

    def fill_data(self, start_dt: datetime):
        print(f"Filling data from {start_dt} to Now...")
        now = datetime.now()
        
        # Determine strict gap
        # If start_dt > now, nothing to do.
        if start_dt > now:
            return

        # We need to buffer data by timestamp.
        # data_buffer[timestamp_obj] = { "timestamp": str, "open": {sym: val}, ... }
        data_buffer: Dict[datetime, Dict[str, Any]] = {}
        lock = threading.Lock()
        
        # Rate Limiting: Hantoo limits (e.g. 20/sec). 
        # We process symbols in chunks or just sleep.
        
        def fetch_symbol(sym):
            curr_date_str = now.strftime("%Y%m%d")
            curr_time_str = now.strftime("%H%M%S")
            
            # Fetch
            _, res = self.hantoo_client.inquire_time_dailychartprice(sym, curr_date_str, curr_time_str)
            if not res: return
            
            output2 = res.get('output2', [])
            
            with lock:
                for r in output2:
                    # r: {'stck_bsop_date': '20240123', 'stck_cntg_hour': '130000', ...}
                    try:
                        r_date = r['stck_bsop_date']
                        r_time = r['stck_cntg_hour']
                        r_dt = datetime.strptime(f"{r_date}_{r_time}", "%Y%m%d_%H%M%S")
                        
                        # Filter by start_dt
                        if r_dt <= start_dt:
                            continue
                        
                        # Filter forward looking? 
                        # In backfill mode (fill_data), we accept all up to 'now'.
                        # But wait, if we are in 'update' loop, we should drop partial.
                        # Usually backfill fetches historically completed candles.
                        # Hantoo's output usually includes the latest (building) candle if market is open?
                        # We will filter strict future, but keep current minute for now, 
                        # relying on update loop to drop it if it's "incomplete".
                        if r_dt > now:
                            continue

                        if r_dt not in data_buffer:
                            data_buffer[r_dt] = {
                                "timestamp": r_dt.strftime("%Y-%m-%d_%H:%M"), 
                                "open": {}, "high": {}, "low": {}, "close": {}, "volume": {}
                            }
                        
                        # Populate
                        # API returns strings
                        data_buffer[r_dt]["open"][sym] = int(r.get('stck_oprc', 0))
                        data_buffer[r_dt]["high"][sym] = int(r.get('stck_hgpr', 0))
                        data_buffer[r_dt]["low"][sym] = int(r.get('stck_lwpr', 0))
                        data_buffer[r_dt]["close"][sym] = int(r.get('stck_prpr', 0))
                        data_buffer[r_dt]["volume"][sym] = int(r.get('cntg_vol', 0))
                        
                    except (ValueError, KeyError) as e:
                        pass
        
        # Execute threaded
        max_workers = 5
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # We might want to delay between submissions if list is huge?
            # Or reliance on requests time overhead is enough?
            # Safe bet: chunk symbols and sleep.
            
            chunk_size = 50
            for i in range(0, len(self.symbols), chunk_size):
                chunk = self.symbols[i:i+chunk_size]
                list(executor.map(fetch_symbol, chunk))
                time.sleep(0.5) # Slight throttle
                
        # Upload
        self._upload_buffer(data_buffer)

    def _upload_buffer(self, data: Dict[datetime, Dict[str, Any]]):
        if not data:
            print("No data to upload.")
            return

        sorted_timestamps = sorted(data.keys())
        now = datetime.now()
        
        for ts in sorted_timestamps:
            # Drop logic: 
            # If timestamp is very close to Now (e.g. current minute), assume incomplete.
            # Hantoo timestamp is "HHMMSS". If Now is 10:00:30 and ts is 10:00:00 (or 10:01:00?), 
            # usually minute candles are stamped at end? 
            # If so, 10:01:00 candle arrives at 10:01:00.
            # If 'ts' == current minute (ignoring seconds), likely incomplete.
            
            # Simple heuristic: if ts is within last 1 minute of 'now', skip?
            # Or user rule: "drop the forward looking data (usually the last entry)".
            # Since 'data' is aggregated from ALL symbols, the "last entry" in time sorted list 
            # IS the latest minute.
            # We can just drop `sorted_timestamps[-1]` if it looks recent.
            
            if ts == sorted_timestamps[-1]:
                # Check if it is "current" minute.
                if (now - ts).total_seconds() < 60:
                     print(f"Skipping potentially incomplete candle at {ts}")
                     continue

            entry = data[ts]
            
            fname = ts.strftime("%Y%m%d_%H%M%S") + ".jsonl.zstd"
            key = f"{self.prefix}/{fname}"
            
            try:
                json_line = json.dumps(entry, default=str)
                cctx = zstd.ZstdCompressor()
                compressed = cctx.compress(json_line.encode('utf-8'))
                
                print(f"Uploading {key}...")
                # self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=compressed)
                self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=compressed)
                
                # Update last timestamp
                if self.last_timestamp is None or ts > self.last_timestamp:
                    self.last_timestamp = ts
                    
            except Exception as e:
                print(f"Failed to upload {key}: {e}")

    def update(self):
        """
        Public method to trigger a fetch cycle.
        """
        if self.last_timestamp:
            # Rescan from a bit before last timestamp or just last timestamp?
            # If we call this frequently, start_dt should be close to Now.
            self.fill_data(self.last_timestamp)
        else:
            # Should have initialized
            self.init_data_flow()

