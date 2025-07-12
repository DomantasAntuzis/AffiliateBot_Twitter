from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
import csv

# --- Setup Selenium Chrome WebDriver ---
# Make sure you have installed:
# pip install selenium
# Download ChromeDriver from https://chromedriver.chromium.org/downloads and put it in your PATH

def scrape_steamdb():
    url = "https://steamdb.info/stats/globaltopsellers/?cc=us"

    chrome_options = Options()
    chrome_options.add_argument("--headful")  # Show browser window
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    results = []

    # Start the browser
    with webdriver.Chrome(options=chrome_options) as driver:
        driver.get(url)
        time.sleep(3)  # Wait for page to load (increase if needed)

        # Select 5000 from the select field
        try:
            select_elem = driver.find_element(By.ID, "dt-length-0")
            select = Select(select_elem)
            select.select_by_value("500")
            print("Selected 500 rows per page.")
            time.sleep(3)  # Wait for table to update
        except Exception as e:
            print(f"Error selecting 500 rows: {e}")

        # Find the main table (by class name)
        try:
            table = driver.find_element(By.CSS_SELECTOR, ".table-sales")
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                tds = row.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 5:
                    name_td = tds[2]
                    all_texts = name_td.text.split('\n')
                    main_name = all_texts[0].strip()
                    
                    if len(all_texts) > 1:
                        extra = all_texts[1].strip()
                        full_name = f"{main_name} ({extra})"
                    else:
                        full_name = main_name
                    
                    price = tds[4].text.strip()
                    print(full_name, price)
                    results.append({"name": full_name, "price": price})
        except Exception as e:
            print(f"Error: {e}")

    # Create csv_data directory if it doesn't exist
    import os
    csv_dir = "csv_data"
    os.makedirs(csv_dir, exist_ok=True)

    # Write results to CSV file
    csv_path = os.path.join(csv_dir, "steamdb_results.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["name", "price"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    print(f"✅ Results written to {csv_path}")