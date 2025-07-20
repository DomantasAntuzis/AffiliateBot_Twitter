import requests
import os
import zipfile
import csv
from datetime import datetime

def get_products():
    # CJ HTTP credentials
    url_base = "https://datatransfer.cj.com"  # Base URL
    username = "7609708"
    password = "xTt$LErv"

    # Actual file path from your email
    file_path = "/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-20250709.zip"

    url = url_base + file_path

    download_dir = "product_files"
    csv_dir = "csv_data"
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    print(f"Fetching: {url}")

    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        out_file = os.path.join(download_dir, os.path.basename(file_path))
        with open(out_file, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded and saved as {out_file}")

        # Extract the zip file
        try:
            with zipfile.ZipFile(out_file, 'r') as zip_ref:
                zip_ref.extractall(download_dir)
                print("📦 Extracted files:")
                for name in zip_ref.namelist():
                    print(f" - {os.path.join(download_dir, name)}")
        except zipfile.BadZipFile:
            print("❌ The downloaded file is not a valid zip archive.")
            exit(1)

        # Find all CSV/TXT files in the extracted directory
        data_files = []
        for fname in os.listdir(download_dir):
            if fname.lower().endswith(".csv") or fname.lower().endswith(".txt"):
                fpath = os.path.join(download_dir, fname)
                if os.path.isfile(fpath):
                    data_files.append(fpath)
        
        if not data_files:
            print("❌ No CSV or TXT files found in product_files/")
            exit(1)
        
        print(f"Found {len(data_files)} data files to process:")
        for df in data_files:
            print(f" - {df}")
        
        # Create organized CSV in csv_data directory combining all files  
        fields = ["PROGRAM_NAME", "ID", "TITLE",  "LINK", "IMAGE_LINK", "AVAILABILITY", "PRICE", "SALE_PRICE"]
        organized_csv = os.path.join(csv_dir, "products_info.csv")
        
        with open(organized_csv, "w", newline='', encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            total_rows = 0
            for data_file in data_files:
                print(f"Processing: {data_file}")
                file_rows = 0
                try:
                    with open(data_file, newline='', encoding="utf-8") as infile:
                        reader = csv.DictReader(infile)
                        for row in reader:
                            out_row = {field: (row.get(field, "").replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()) for field in fields}
                            writer.writerow(out_row)
                            file_rows += 1
                            total_rows += 1
                    print(f"  ✅ Processed {file_rows} rows from {os.path.basename(data_file)}")
                except Exception as e:
                    print(f"  ❌ Error processing {data_file}: {e}")
        
        print(f"✅ Combined CSV written to: {organized_csv}")
        print(f"✅ Total rows processed: {total_rows} from {len(data_files)} files")
        
        # Clean up temporary files in product_files directory
        print("🧹 Cleaning up temporary files...")
        for fname in os.listdir(download_dir):
            fpath = os.path.join(download_dir, fname)
            if os.path.isfile(fpath) and not fname.startswith("."):
                try:
                    os.remove(fpath)
                    print(f"Deleted: {fname}")
                except Exception as e:
                    print(f"Could not delete {fname}: {e}")
        print("✅ Cleanup complete")
    else:
        print(f"❌ Failed to download. Status: {response.status_code}")
        print(response.text) 