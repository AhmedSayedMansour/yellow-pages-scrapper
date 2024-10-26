import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime

app = FastAPI()

# MongoDB connection function
def get_mongodb_collection(uri, db_name, collection_name):
    client = MongoClient(uri)
    return client[db_name][collection_name]

# Scraper function
def scrape_yellowpages(keyword, size):
    url = f"https://yellowpages.com.eg/en/keyword/{keyword}"
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to retrieve data from YellowPages")

    soup = BeautifulSoup(response.text, 'html.parser')
    companies = []

    for company_div in soup.select(".company-list-item")[:size]:
        companies.append({
            "name": company_div.select_one(".company-name").get_text(strip=True),
            "phone": company_div.select_one(".phone-number").get_text(strip=True),
            "location": company_div.select_one(".company-address").get_text(strip=True),
            "description": company_div.select_one(".company-description").get_text(strip=True),
        })

    return companies

# API endpoint
@app.get("/scrape/")
async def get_companies(keyword: str, size: int = 30):
    # Ensure size is within bounds
    size = min(max(size, 30), 100)
    today = datetime.now().strftime("%Y-%m-%d")

    # MongoDB collection
    collection = get_mongodb_collection("mongodb://localhost:27017", "yellowpages_data", "companies")

    # Check cached data
    cached_data = list(collection.find({"keyword": keyword, "scraped_date": today}))
    if cached_data:
        return cached_data

    # Scrape new data if no cache exists
    companies = scrape_yellowpages(keyword, size)
    if not companies:
        raise HTTPException(status_code=404, detail="No data found for the specified keyword.")

    # Add metadata and insert to MongoDB
    for company in companies:
        company.update({"keyword": keyword, "scraped_date": today})
    collection.insert_many(companies)

    return companies
