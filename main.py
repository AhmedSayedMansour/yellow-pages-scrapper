import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime

app = FastAPI()

# MongoDB Connection
def get_mongodb_collection(host, db, collection_name):
    client = MongoClient(host)
    db = client[db]
    collection = db[collection_name]

    return collection

def scrape_yellowpages(keyword, size):
    # URL for scraping based on keyword
    url = f"https://yellowpages.com.eg/en/keyword/{keyword}"
    print("url: ", url)
    response = requests.get(url)
    print("status_code: ", response.status_code)
    print("response: ", response.text)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Example: find all company containers
    companies = []
    for company_div in soup.select(".company-list-item")[:size]:  # Adjust selector
        name = company_div.select_one(".company-name").get_text(strip=True)
        phone = company_div.select_one(".phone-number").get_text(strip=True)
        location = company_div.select_one(".company-address").get_text(strip=True)
        description = company_div.select_one(".company-description").get_text(strip=True)
        
        company_data = {
            "name": name,
            "phone": phone,
            "description": description,
            "location": location,
            "scraped_date": datetime.now().strftime("%Y-%m-%d")
        }
        
        # Append company data to the result list
        companies.append(company_data)
    
    return companies

@app.get("/scrape/")
async def get_companies(keyword: str, size: int = 30):

    collection = get_mongodb_collection("localhost:27017", "yellowpages_data", "companies")
    # Validate size parameter
    size = min(max(size, 30), 100)  # Ensure size is between 30 and 100
    
    # Check cache for the same day results
    today = datetime.now().strftime("%Y-%m-%d")
    cached_data = list(collection.find({"keyword": keyword, "scraped_date": today}))
    
    if cached_data:
        return cached_data  # Return cached data if available
    
    # Perform new scrape if no cache exists
    companies = scrape_yellowpages(keyword, size)

    print("companies: ", companies)
    
    # Add keyword and scraped_date to each company data (Avoid repetations)
    for company in companies:
        company.update({"keyword": keyword, "scraped_date": today})
        
    # Insert new scrape result into MongoDB
    if len(companies) > 0:
        collection.insert_many(companies)
    
    return companies

