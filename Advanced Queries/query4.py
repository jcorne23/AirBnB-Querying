from pymongo import MongoClient

client = MongoClient("mongodb+srv://cellistkyle:yo@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]

neighborhood_docs = db["neighbourhoods"].find({}, {"neighbourhood": 1})
all_neighborhoods = set(doc["neighbourhood"] for doc in neighborhood_docs)

target_month = "2025-07"  # July 2025

available_listing_ids = db["calendar"].distinct(
    "listing_id",
    {
        "available": True,
        "date": {"$regex": f"^{target_month}"}
    }
)

available_listings = db["listings"].find(
    {"id": {"$in": available_listing_ids}},
    {"neighbourhood_cleansed": 1}
)
active_neighborhoods = set(
    doc["neighbourhood_cleansed"] for doc in available_listings if "neighbourhood_cleansed" in doc
)
neighborhoods_with_no_listings = all_neighborhoods - active_neighborhoods

# Output
print(f"Portland Neighborhoods with NO available listings in {target_month}:\n")
for n in sorted(neighborhoods_with_no_listings):
    print(f"- {n}")