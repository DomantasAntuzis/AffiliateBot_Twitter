import csv
import json

def find_deals():
	productFile = "csv_data/products_info.csv"
	steamdbFile = "csv_data/steamdb_results.csv"

	#read product file
	PFile = open(productFile, encoding="utf-8")
	PFileReader = csv.reader(PFile)
	PFileList = list(PFileReader)

	# print(PFileList[1][2])

	#read steamdb file
	SFile = open(steamdbFile, encoding="utf-8")
	SFileReader = csv.reader(SFile)
	SFileList = list(SFileReader)

	#find common top sellers from both files
	commonGameTittles = []
	for row in SFileList:
		target_value = row[0]
		if any(target_value in p_row[2] for p_row in PFileList):
			commonGameTittles.append(target_value)

	# Group deals by game title, keeping all sources
	gameDealsDict = {}
	for i in range(len(PFileList)):
		if (PFileList[i][2] in commonGameTittles and (PFileList[i][5] == "in stock" or PFileList[i][5] == "in_stock")):
			title = PFileList[i][2]
			price = float(PFileList[i][6].replace(" USD", ""))
			salePrice = float(PFileList[i][7].replace(" USD", "")) if PFileList[i][7] != "" else ""
			source = PFileList[i][0]  # GOG, Yuplay, etc.
			
			if title not in gameDealsDict:
				gameDealsDict[title] = []
			
			if salePrice != "":
				discount = f"{round((price - salePrice) / price * 100)}%"
				gameObj = {
					"tittle": title,
					"price": price,
					"link": PFileList[i][3],
					"image_link": PFileList[i][4],
					"salePrice": salePrice,
					"discount": discount,
					"source": source
				}
				gameDealsDict[title].append(gameObj)

	# Find deals cheaper than Steam, showing all sources
	deals = []
	for title, sourcesDeals in gameDealsDict.items():
		for s_row in SFileList:
			if title == s_row[0] and s_row[1] != "Free":
				steamPrice = float(s_row[1].replace("$", ""))
				
				# Check each source for this game
				for deal in sourcesDeals:
					currentPrice = deal["price"]
					currentSalePrice = deal["salePrice"]
					effectivePrice = currentSalePrice if currentSalePrice != "" else currentPrice
					
					if effectivePrice < steamPrice:
						deals.append(deal)
				break

	#remove duplicates
	deal_counts = {}
	for deal in deals:
			deal_key = f"{deal['tittle']}_{deal['source']}"
			deal_counts[deal_key] = deal_counts.get(deal_key, 0) + 1

	# Second pass: keep only deals that appear exactly once
	filtered_deals = []
	for deal in deals:
			deal_key = f"{deal['tittle']}_{deal['source']}"
			if deal_counts[deal_key] == 1:  # Only keep if it appears exactly once
					filtered_deals.append(deal)

	deals = filtered_deals

				#reikia patikrinti ar nuolaida egzistuoja
				
	datafile = open("deals.json", "w", encoding="utf-8")
	json.dump(deals, datafile, ensure_ascii=False, indent=4)
	datafile.close()






