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

	deals = []

	#collect games from product list with the same title as steamdb top 500

	for s_row in SFileList:
		target_title = s_row[0]
		for p_row in PFileList:
			if target_title == p_row[2] and (p_row[5] == "in stock" or p_row[5] == "in_stock"):
				gameObj = {
					"source": p_row[0],
					"title": p_row[2],
					"link": p_row[3],
					"image_link": p_row[4],
				}
				deals.append(gameObj)
				# print(p_row)

	PFile.close()
	SFile.close()

	return deals






