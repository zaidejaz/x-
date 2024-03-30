import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'referer': 'https://www.zillow.com/homes/Missoula,-MT_rb/'
}

# Function to extract data from a single page
def extract_data_from_page(url):
    try:
        response = requests.get(url, headers=header, proxies=proxies)
        response.raise_for_status()
        html = response.text
    except Exception as e:
        logging.error(f"Error making GET request to URL: {url}\nError message: {e}")
        return []

    # Parse the HTML
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception as e:
        logging.error(f"Error parsing HTML: {e}")
        return []

    # Find all table rows
    rows = soup.find_all('tr', class_='StyledTableRow-c11n-8-99-1__sc-65t1u6-0 hfWgOM')

    # Initialize list to store data from current page
    data = []

    # Loop through each row and extract name and phone number
    for row in rows:
        name_element = row.find('a')
        if name_element is not None:
            name = name_element.text.strip()
        else:
            name = "Name not found"

        phone_element = row.find('div', class_='Text-c11n-8-99-1__sc-aiai24-0 bwCmyj')
        if phone_element is not None:
            phone_text = phone_element.text.strip()
            # Remove the text "phone number" and leading/trailing spaces
            phone_text = phone_text.replace('phone number', '').strip()
            phone = phone_text
        else:
            phone = "Phone not found"

        if phone != "Phone not found" and name != "Name not found":
            data.append([name, phone, zip_code])

    return data

# Read the zip codes from the Excel file
logging.info("Reading zip codes from the Excel file...")
try:
    zip_codes_df = pd.read_excel('zipcodes.xlsx')
    zip_codes = zip_codes_df['Zip Codes'].tolist()
except Exception as e:
    logging.error(f"Error reading zip codes from the Excel file: {e}")
    exit()

# Initialize an empty list to store data
all_data = []

# Iterate through each zip code
for zip_code in zip_codes:
    logging.info(f"Processing zip code: {zip_code}")

    # Modify the base URL to include the current zip code
    base_url = f'https://www.zillow.com/professionals/real-estate-agent-reviews/{zip_code}/'

    # Find total number of pages for the current zip code
    try:
        response = requests.get(base_url, headers=header)
        response.raise_for_status()
        html = response.text
    except Exception as e:
        logging.error(f"Error making GET request to URL: {base_url}\nError message: {e}")
        continue

    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.find('nav', class_='StyledPagination-c11n-8-99-1__sc-4uav85-0')

    if pagination:
        last_page = pagination.find_all('li')[-2].text
        total_pages = int(last_page)
    else:
        total_pages = 1

    # Scrape data from each page for the current zip code
    for page_number in range(1, total_pages + 1):
        page_url = f'{base_url}?page={page_number}'
        logging.info(f"Processing page {page_number} for zip code: {zip_code}")
        page_data = extract_data_from_page(page_url)
        all_data.extend(page_data)
        time.sleep(10)  # Add a delay to avoid rate limiting

# Create a DataFrame
logging.info("Creating DataFrame...")
df = pd.DataFrame(all_data, columns=['Name', 'Phone', 'Zip Code'])

file_name = "data.xlsx"

# Save DataFrame to Excel file
logging.info("Saving DataFrame to Excel file...")
try:
    df.to_excel(file_name, index=False)
    logging.info(f"Data saved to {file_name} file.")
except Exception as e:
    logging.error(f"Error saving DataFrame to Excel file: {e}")
