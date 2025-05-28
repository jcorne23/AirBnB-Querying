from pymongo import MongoClient
import pandas as pd

# Step 1: Load and clean CSV
df = pd.read_csv("listings.csv")

# Drop completely empty or useless columns
df.drop(columns=["calendar_updated", "neighbourhood_group_cleansed"], inplace=True)

# Clean price column
df["price"] = df["price"].replace('[\$,]', '', regex=True)
df["price"] = pd.to_numeric(df["price"], errors="coerce")

# Drop rows missing key data
df = df.dropna(subset=["price", "name", "bedrooms", "bathrooms"])

# Optional fills
df["host_about"] = df["host_about"].fillna("Not provided")
df["reviews_per_month"] = df["reviews_per_month"].fillna(0.0)

# Convert 't' and 'f' to boolean True/False in object columns
bool_map = {"t": True, "f": False}
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].replace(bool_map)

df["source"] = "dev-test"
df["loc"] = "Portland"   
# Convert to dict for MongoDB
docs = df.to_dict(orient="records")

# Step 2: Insert into MongoDB
client = MongoClient("mongodb+srv://cellistkyle:@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
collection = client["airbnb"]["listings"]
collection.insert_many(docs)

print(f"Inserted {len(docs)} documents into MongoDB.")
