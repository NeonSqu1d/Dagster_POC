import pandas as pd

import dagster as dg

@dg.asset
def processed_data():
	df = pd.read_csv("data/sample_data.csv")

	df["age_group"] = pd.cut(
		df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
	)

	df.to_csv("data/processed_data.csv", index=False)
	return "Data loaded successfully"


defs = dg.Definitions(assets=[processed_data])


