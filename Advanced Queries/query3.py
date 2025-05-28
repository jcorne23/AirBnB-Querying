from pymongo import MongoClient

client = MongoClient("mongodb+srv://cellistkyle:yo@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]

pipeline = [
    {"$match": {"date": {"$regex": "-12-"}}},
    {
        "$group": {
            "_id": {
                "city": "$rloc",
                "year": {"$substr": ["$date", 0, 4]}
            },
            "review_count": {"$sum": 1}
        }
    },
    {"$sort": {"_id.year": 1, "_id.city": 1}}
]

results = db["reviews"].aggregate(pipeline)

print("December Review Counts by City and Year:\n")
for doc in results:
    year = doc["_id"]["year"]
    city = doc["_id"]["city"]
    count = doc["review_count"]
    print(f"{year} - {city}: {count} reviews")