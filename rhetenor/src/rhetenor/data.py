
import os
import yaml
import boto3
import zstandard as zstd
import json
import io
from datetime import datetime
from typing import Iterator, Dict, Any, Optional

class DataLoader:
    def __init__(self, bucket: str, prefix: str, auth_config_path: str = "auth/aws.yaml", region: Optional[str] = None):
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
