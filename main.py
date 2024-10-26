import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

app = FastAPI()

def connect_to_mongodb(uri, db_name, collection_name):
    client = MongoClient(uri)
    return client[db_name][collection_name]

def fetch_yellowpages_data(search_term, max_results):
    url = f"https://yellowpages.com.eg/en/keyword/{search_term}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to retrieve data from YellowPages")

    soup = BeautifulSoup(response.text, 'html.parser')
    companies = []

    for company in soup.select(".row.item-row")[:max_results]:
        company_name = company.select_one(".item-title").get_text(strip=True) if company.select_one(".item-title") else "N/A"
        address = company.select_one(".address-text").get_text(strip=True) if company.select_one(".address-text") else "N/A"
        description = company.select_one(".item-aboutUs a").get_text(strip=True) if company.select_one(".item-aboutUs a") else "N/A"
        
        phone = company.select_one(".call-us-click").get_text(strip=True) if company.select_one(".call-us-click") else "N/A"
        website = company.select_one(".website")['href'] if company.select_one(".website") else "N/A"
        email_link = company.select_one(".tab-mail")['href'] if company.select_one(".tab-mail") else "N/A"

        categories = [cat.get_text(strip=True) for cat in company.select(".category")]
        keywords = [kw.get_text(strip=True) for kw in company.select(".two-words span")]

        company_data = {
            "company_name": company_name,
            "address": address,
            "description": description,
            "phone": phone,
            "website": website,
            "email_link": email_link,
            "categories": categories,
            "keywords": keywords,
            "scraped_date": datetime.now().strftime("%Y-%m-%d")
        }
        
        companies.append(company_data)

    return companies

def convert_objectid_to_str(data):
    """Recursively convert ObjectId fields to strings in MongoDB documents."""
    if isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    if isinstance(data, dict):
        return {key: (str(value) if isinstance(value, ObjectId) else convert_objectid_to_str(value))
                for key, value in data.items()}
    return data

@app.get("/scrape/")
async def get_companies(search_term: str, max_results: int = 30):
    max_results = min(max(max_results, 30), 100)
    today = datetime.now().strftime("%Y-%m-%d")
    
    collection = connect_to_mongodb("mongodb://localhost:27017", "yellowpages_db", "company_data")
    cached_results = list(collection.find({"search_term": search_term, "scraped_date": today}))
    
    if cached_results:
        return convert_objectid_to_str(cached_results)
    
    companies = fetch_yellowpages_data(search_term, max_results)
    if not companies:
        raise HTTPException(status_code=404, detail="No results found.")

    for company in companies:
        company.update({"search_term": search_term, "scraped_date": today})
    collection.insert_many(companies)

    return companies
