
import sys
import os

# Ensure src is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from rhetenor.data import download_kospi_master

def main():
    print("Downloading and parsing KOSPI master...")
    try:
        master = download_kospi_master(verbose=True)
        print(f"Total items: {len(master)}")
        
        # Print first 3 items
        print("\n--- Sample Items ---")
        count = 0
        for code, info in master.items():
            print(f"[{code}]: {info}")
            count += 1
            if count >= 3:
                break
                
        # Check specific known stock if possible, e.g. Samsung Electronics '005930'
        target = '005930'
        if target in master:
            print(f"\n[{target}] Samsung Electronics:")
            print(master[target])
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
