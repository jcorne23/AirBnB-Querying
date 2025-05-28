from pymongo import MongoClient
import pandas as pd

df = pd.read_csv("neighbourhoods.csv", usecols=["neighbourhood"])  # portland done, #

# # Add 'rloc' field
df["rloc"] = "Portland"

# # Convert to dict records
docs = df.to_dict(orient="records")

# # Connect and insert into MongoDB
client = MongoClient("mongodb+srv://cellistkyle:yo@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]
db["neighbourhoods"].insert_many(docs)

print(f"Inserted {len(docs)} neighborhoods into MongoDB.")