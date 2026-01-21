
import os
import sys
import pytest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
import json
import zstandard as zstd
import io

# Ensure src is in path to import rhetenor
sys.path.append(os.path.join(os.getcwd(), 'rhetenor/src'))

from rhetenor.data import DataLoader

@pytest.fixture
def mock_auth_file(tmp_path):
    """Create a temporary auth file."""
    auth_file = tmp_path / "aws.yaml"
    auth_content = """
region: ap-northeast-2
access_key_id: TEST_KEY
secret_access_key: TEST_SECRET
"""
    auth_file.write_text(auth_content)
    return str(auth_file)

@pytest.fixture
def mock_boto3():
    with patch('rhetenor.data.boto3') as mock:
        yield mock

def test_dataloader_init(mock_auth_file, mock_boto3):
    """Test DataLoader initialization from config."""
    loader = DataLoader(bucket="test-bucket", prefix="logs", auth_config_path=mock_auth_file)
    
    assert loader.bucket == "test-bucket"
    assert loader.prefix == "logs"
    assert loader.auth_config['region'] == "ap-northeast-2"
    
    # Check boto3 client called with correct params
    mock_boto3.client.assert_called_once_with(
        's3',
        region_name='ap-northeast-2',
        aws_access_key_id='TEST_KEY',
        aws_secret_access_key='TEST_SECRET'
    )

def test_list_objects(mock_auth_file, mock_boto3):
    """Test listing objects with date filtering."""
    loader = DataLoader(bucket="test-bucket", prefix="logs", auth_config_path=mock_auth_file)
    
    # Mock paginator
    mock_paginator = MagicMock()
    mock_boto3.client.return_value.get_paginator.return_value = mock_paginator
    
    # Mock response
    mock_paginator.paginate.return_value = [
        {
            'Contents': [
                {'Key': 'logs/20230101_100000_uuid1.jsonl.zstd'},
                {'Key': 'logs/20230102_100000_uuid2.jsonl.zstd'},
                {'Key': 'logs/garbage_file.txt'},
                {'Key': 'logs/20230103_100000_uuid3.jsonl.zstd'},
            ]
        }
    ]
    
    # Test all
    files = list(loader.list_objects())
    assert len(files) == 3
    assert 'logs/garbage_file.txt' not in files
    
    # Test filtering
    start = datetime(2023, 1, 2, 0, 0, 0)
    end = datetime(2023, 1, 2, 23, 59, 59)
    filtered = list(loader.list_objects(start_date=start, end_date=end))
    assert len(filtered) == 1
    assert filtered[0] == 'logs/20230102_100000_uuid2.jsonl.zstd'

@pytest.fixture
def sample_zstd_data():
    """Create sample zstd compressed jsonl data."""
    data = [
        {"id": 1, "msg": "hello"},
        {"id": 2, "msg": "world"}
    ]
    jsonl_str = "\n".join(json.dumps(r) for r in data)
    
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(jsonl_str.encode('utf-8'))
    return compressed, data

def test_download_and_parse(mock_auth_file, mock_boto3, sample_zstd_data):
    """Test downloading and parsing data."""
    compressed_data, expected_data = sample_zstd_data
    
    loader = DataLoader(bucket="test-bucket", prefix="logs", auth_config_path=mock_auth_file)
    
    # Mock get_object response body
    # We need to return a stream-like object that has a read method
    # But boto3 body is usually a StreamingBody which we can simulate with BytesIO
    
    mock_body = MagicMock()
    # When stream_reader(body) is called, it expects a file-like object.
    # We can just use BytesIO directly if we mock the response dict.
    
    mock_response = {
        'Body': io.BytesIO(compressed_data)
    }
    mock_boto3.client.return_value.get_object.return_value = mock_response
    
    results = list(loader.download_and_parse("some_key"))
    
    assert len(results) == 2
    assert results == expected_data
    mock_boto3.client.return_value.get_object.assert_called_with(Bucket="test-bucket", Key="some_key")
