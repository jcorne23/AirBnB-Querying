from pymongo import MongoClient
from pprint import pprint

client = MongoClient("mongodb+srv://cellistkyle:yo@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]

# Step 1: Get IDs of "Entire home/apt" listings
entire_home_ids = db["listings"].distinct("id", {"room_type": "Entire home/apt"})

# Step 2: Aggregate calendar availability for those listings
pipeline = [
    {
        "$match": {
            "listing_id": {"$in": entire_home_ids},
            "available": True,
            "date": {"$regex": "^2025-(03|04|05|06|07|08)"}  # March–August
        }
    },
    {
        "$group": {
            "_id": {"$substr": ["$date", 0, 7]},  # Group by "YYYY-MM"
            "available_nights": {"$sum": 1}
        }
    },
    {"$sort": {"_id": 1}}
]

result = list(db["calendar"].aggregate(pipeline))

# Step 3: Print results
print("Available Nights per Month (Entire home/apt):\n")
for entry in result:
    print(f"{entry['_id']}: {entry['available_nights']} nights")
