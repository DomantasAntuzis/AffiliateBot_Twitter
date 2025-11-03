# with open("csv_data/products_info.csv", encoding="utf-8") as f:
# 	reader = csv.reader(f)
# 	for row in reader:
		

import os
import requests
import json

RAWG_API_KEY = os.getenv("RAWG_KEY")

params = {
    'key': RAWG_API_KEY,
    'platforms': 4,
    'page_size': 100
}

resp_games = requests.get("https://api.rawg.io/api/games", params=params)
# resp_games = requests.get("https://api.rawg.io/api/platforms", params=params)
games_data = resp_games.json()

with open("test.json", "w") as f:
	json.dump(games_data, f, indent=4)