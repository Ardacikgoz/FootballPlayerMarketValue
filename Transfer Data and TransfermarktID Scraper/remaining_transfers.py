import pandas as pd

df = pd.read_csv("transferData.csv",names = ["query", "seasonName", "date", "oldClub", "newClub", "marketValue", "transferFee"])
print(len(pd.unique(df["query"])))
names=["Query", "Season_Name", "Date", "Old_Club", "New_Club", "Market_Value", "Transfer_Fee"]
df = pd.read_csv("transferData.csv", header=None)

# Assign the header to the DataFrame
df.columns = names

# Save the DataFrame back to the CSV file with the new header
df.to_csv("transferData.csv", index=False)
