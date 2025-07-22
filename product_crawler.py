import asyncio
import json, os, csv
import aiohttp
from bs4 import BeautifulSoup
from utils import load_json, save_json


json_file = "fixed_products.json"


async def main():
    failed_products = load_json(json_file)
    #print(failed_products[0])
    failed_again = await crawl_urls(failed_products)
    save_json("failed_products2.json", failed_again)
    print(f"Retry complete. {len(failed_again)} products still failed.")

async def crawl_urls(products, batch_size=100, output_file="enriched_products.csv"):
    header = ["product_id", "product_name", "url"]
    failed_products = []

    # create the file first if not exist
    if not os.path.exists(output_file):
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
    # fetch the names in 100-size batches
    for i in range(0, len(products), batch_size):
        async with aiohttp.ClientSession() as session:
            product_names = [get_pname(product["url"], session) for product in products[i:i+batch_size]] 
            product_names = await asyncio.gather(*product_names, return_exceptions=True)
        # Save the fetched 100 names to the csv file
        batch = products[i:i + batch_size]
        with open(output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for product, name in zip(batch, product_names):
                product_id = product["_id"]
                url = product["url"]
                if isinstance(name, str):
                    writer.writerow([product_id, name, url]) 
                else:
                    failed_products.append({"product_id": product_id, "url": url})
                
        print(f"Saved batch starting at index {i}")
    return failed_products

async def get_pname(urls, session):
    for url in urls:
        try:
            async with session.get(url) as response:
        #Get the pname from response
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    product_name = soup.find("span", class_="base", attrs={"data-ui-id": "page-title-wrapper"})
                    if product_name:
                        return product_name.text.strip()
                    else:
                        print(f"Product name not found on {url}")
                        continue
                else:
                    continue

        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

    return None  # all urls failed

if __name__ == "__main__":
    asyncio.run(main())
