from pymongo import MongoClient

client = MongoClient("mongodb+srv://cellistkyle:yo@cluster0.jtzdio9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["airbnb"]

# Dates to check
start_date = "2025-03-05"
end_date = "2025-03-06"

sample_vals = db["calendar"].distinct("available")
print("Distinct 'available' values:", sample_vals)

# Check what date formats exist
sample_dates = db["calendar"].distinct("date")
print("Sample 'date' values:", sample_dates[:10])  # Just print 10
# Step 1: Find listings available on both days
available_cursor = db["calendar"].aggregate([
    {
        "$match": {
            "date": {"$in": [start_date, end_date]},
            "available": True
        }
    },
    {
        "$group": {
            "_id": "$listing_id",
            "count": {"$sum": 1}
        }
    },
    {
        "$match": {"count": 2}  # Ensure both dates are available
    }
])



# Collect matching listing_ids

listing_ids = [doc["_id"] for doc in available_cursor]
print(f"Found {len(listing_ids)} listings available on both dates.")
print(listing_ids[:10])  # Preview first few

results = db["listings"].find(
    {
        "id": {"$in": listing_ids},
        "loc": "Portland",

    },
    {
        "name": 1,
        "neighbourhood_cleansed": 1,
        "room_type": 1,
        "accommodates": 1,
        "property_type": 1,
        "amenities": 1,
        "price": 1,
        "review_scores_rating": 1
    }
).sort("review_scores_rating", -1).limit(10)

#for listing in results:
#    print(listing)

# Step 3: Display
with open("top_listings.txt", "w", encoding="utf-8") as f:
    f.write("Top 10 Listings for 2025-03-05 and 2025-03-06\n\n")
    for i, listing in enumerate(results, 1):
        f.write(f"{i}. {listing.get('name', 'Unnamed')}\n")
        f.write(f"   Neighborhood: {listing.get('neighbourhood_cleansed', 'N/A')}\n")
        f.write(f"   Room Type: {listing.get('room_type', 'N/A')}\n")
        f.write(f"   Accommodates: {listing.get('accommodates', 'N/A')}\n")
        f.write(f"   Property Type: {listing.get('property_type', 'N/A')}\n")
        f.write(f"   Price: ${listing.get('price', 'N/A')}\n")
        f.write(f"   Rating: {listing.get('review_scores_rating', 'N/A')}\n")
        amenities = listing.get("amenities", [])
        if isinstance(amenities, list):
            amenities_str = ", ".join(str(item).strip() for item in amenities)
        else:
            amenities_str = str(amenities).strip()
        f.write(f"   Amenities: {amenities_str}\n\n")

print("Exported top 10 listings to top_listings.txt")