import argparse
import json
import os
import requests
import time
from typing import List, Dict, Any
from datetime import datetime
import hashlib
from glob import glob
from collections import defaultdict


def load_stdlib_doc(path: str) -> Dict:
    headers = ["1. Type signature", "2. Arguments",
               "3. Semantic Definition", "4. Example"]
    with open(path, "rt") as f:
        lines = [x.strip() for x in f.readlines()]
    segments = defaultdict(lambda : [])
    header = "title"
    for line in lines:
        if line in headers:
            header = line
        else:
            segments[header].append(line)
    segments = {k: "\n".join(v) for k, v in segments.items()}
    result = {
        "name": os.path.splitext(os.path.split(path)[-1])[0],
        "description": segments.get("3. Semantic Definition", ""),
        "syntax": segments.get("1. Type signature", "") + "\n" + segments.get("2. Arguments", ""),
        "example": segments.get("4. Example", "")
    }
    return result


def query_ollama(endpoint: str, model: str, prompt: str, system_prompt: str = None, temperature=0.0) -> Dict[str, Any]:
    """
    Sends a request to the Ollama API (compatible with OpenWebUI).
    """
    # Ensure endpoint ends with the correct path if not provided
    # Standard Ollama generates at /api/generate (for raw completion)
    # or /api/chat (for chat messages). Using /api/chat is usually safer for modern models.

    url = f"{endpoint.rstrip('/')}/api/chat"

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "stream": False,  # We want the whole response at once
        "options": {
            "temperature": temperature,  # Keep it deterministic for extraction tasks
            # "num_ctx": 4096     # Adjust context window if necessary
        }
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling LLM: {e}")
        return {"error": str(e), "message": {"content": ""}}


def hasher(data):
    return hashlib.sha1(data.encode('utf-8')).hexdigest()

def check_exist(output_dir, output_name):
    for ext in [".txt",".json"]:
        output_filename = f"{os.path.basename(output_name)}{ext}"
        output_path = os.path.join(output_dir, output_filename)
        if os.path.exists(output_path):
            return output_path
    return None

def check_save(output_dir, output_name, content):
    ext = ".json" if type(content) == dict else ".txt"
    output_filename = f"{os.path.basename(output_name)}{ext}"
    output_path = os.path.join(output_dir, output_filename)
    if os.path.exists(output_path):
        raise Exception(f"Output file {output_path} exists. Not overwriting.")
    else:
        if type(content) == str:
            with open(output_path, 'wt', encoding='utf-8') as f:
                f.write(content)
        elif type(content) == dict:
            with open(output_path, 'wt', encoding='utf-8') as f:
                json.dump(content, f, indent=4, ensure_ascii=False)
        else:
            raise Exception(f"Could not save {type(content)}")
    return output_path
