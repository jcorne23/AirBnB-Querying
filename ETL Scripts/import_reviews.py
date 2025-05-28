import pandas as pd
from pymongo import MongoClient


# # Load CSV
df = pd.read_csv("sdreviews.csv")  # portland done, #

# # Add 'rloc' field
df["rloc"] = "SD"

# # Convert to dict records
docs = df.to_dict(orient="records")

# # Connect and insert into MongoDB
client = MongoClient("mongodb+srv://cellistkyle:@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]
db["reviews"].insert_many(docs)

print(f"Inserted {len(docs)} reviews into MongoDB.")
