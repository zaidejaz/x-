import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}

# Function to extract data from a single page
def extract_data_from_page(url, zip_code):
    try:
        response = requests.get(url, headers=header)
        response.raise_for_status()
        html = response.text
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logging.warning(f"Page not found for zip code {zip_code}. Skipping...")
            return []
        elif response.status_code == 403:
            logging.warning(f"Access forbidden for zip code {zip_code}. Retrying in 1 minute...")
            time.sleep(60)  # Wait for 1 minute before retrying
            return extract_data_from_page(url, zip_code)  # Retry recursively
        else:
            logging.error(f"Error making GET request to URL: {url}\nError message: {e}")
            return []
    except Exception as e:
        logging.error(f"Error making GET request to URL: {url}\nError message: {e}")
        return []

    # Parse the HTML
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception as e:
        logging.error(f"Error parsing HTML: {e}")
        return []

    # Extract agent data
    agent_data = []
    agent_cards = soup.find_all('div', class_='jsx-3873707352 card-details')
    for card in agent_cards:
        name = card.find('span', class_='jsx-3873707352 text-bold').text.strip()
        phone_icon_span = card.find('span', class_='jsx-3873707352 phone-icon')
        if phone_icon_span:
            telephone_div = phone_icon_span.find('div', class_='jsx-3873707352 agent-phone hidden-xs hidden-xxs')
            if telephone_div:
                telephone = telephone_div.text.strip()
            else:
                telephone = None
        else:
            telephone = None
        agent_data.append({'Name': name, 'Phone': telephone, 'Zip Code': zip_code})
    
    return agent_data

# Main function
def main():
    # Read zip codes from the Excel file
    try:
        zip_codes_df = pd.read_excel('zipcodes.xlsx')
        zip_codes = zip_codes_df['Zip Codes'].tolist()
    except Exception as e:
        logging.error(f"Error reading zip codes from the Excel file: {e}")
        return

    base_url = "https://www.realtor.com/realestateagents/"
    print("Zip Codes: ", zip_codes)
    for zip_code in zip_codes:
        logging.info(f"Fetching data for zip code: {zip_code}")
        page_number = 1  # Reset page number for each zip code
        
        while True:
            page_url = f"{base_url}{zip_code}/pg-{page_number}"  # Corrected URL construction
            logging.info(f"Fetching URL: {page_url}")
            logging.info(f"Scraping data from page {page_number} for zip code {zip_code}...")

            agent_data = extract_data_from_page(page_url, zip_code)

            # If no data is found on the page, break the loop
            if not agent_data:
                logging.info(f"No data found for zip code {zip_code} on page {page_number}. Moving to next zip code...")
                break

            # Convert data into DataFrame
            df = pd.DataFrame(agent_data)
            
            # Save data to Excel file after processing each page
            file_name = "realtor_output_data_noproxies.xlsx"
            try:
                if not os.path.exists(file_name):
                    df.to_excel(file_name, index=False)
                    logging.info(f"Data saved to new file: {file_name}")
                else:
                    existing_data = pd.read_excel(file_name)
                    updated_data = pd.concat([existing_data, df], ignore_index=True)
                    updated_data.to_excel(file_name, index=False)
                    logging.info(f"Data appended to {file_name} file for zip code {zip_code}.")
            except Exception as e:
                logging.error(f"Error saving DataFrame to Excel file: {e}")
            
            page_number += 1
            time.sleep(10)  # Add a delay to avoid rate limiting

if __name__ == "__main__":
    main()
