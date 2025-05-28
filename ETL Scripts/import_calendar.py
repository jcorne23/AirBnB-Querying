from pymongo import MongoClient
import pandas as pd

# Step 1: Load cleaned calendar data
calendar_df = pd.read_csv("cleaned_calendar.csv")

# Step 2: Connect to MongoDB
client = MongoClient("mongodb+srv://cellistkyle:@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
calendar_collection = client["airbnb"]["calendar"]

# Step 3: Insert records into 'calendar' collection
calendar_docs = calendar_df.to_dict(orient="records")
calendar_collection.insert_many(calendar_docs)

print(f"Inserted {len(calendar_docs)} calendar entries into MongoDB.")
