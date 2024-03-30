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
def extract_data_from_page(url, zip_code, proxy):
    try:
        response = requests.get(url, headers=header, proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"})
        response.raise_for_status()
        html = response.text
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logging.warning(f"Page not found for zip code {zip_code}. Skipping...")
            return [], False
        elif response.status_code == 403:
            logging.error(f"HTTP 403 Forbidden error for URL: {url}. Proxy IP may be blocked.")
            return [], True  # Return flag indicating proxy is blocked
        else:
            logging.error(f"Error making GET request to URL: {url}\nError message: {e}")
            return [], False
    except Exception as e:
        logging.error(f"Error making GET request to URL: {url}\nError message: {e}")
        return [], False

    # Parse the HTML
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception as e:
        logging.error(f"Error parsing HTML: {e}")
        return [], False

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
    
    return agent_data, False

# Main function
def main():
    # Read zip codes from the Excel file
    try:
        logging.info("Reading file for zipcodes....")
        zip_codes_df = pd.read_excel('zipcodes.xlsx')
        zip_codes = zip_codes_df['Zip Codes'].tolist()
    except Exception as e:
        logging.error(f"Error reading zip codes from the Excel file: {e}")
        return

    # Get proxies from the text file
    logging.info("Getting proxies from the text file....")
    with open('iproyal-proxies.txt') as f:
        proxies = f.read().splitlines()

    base_url = "https://www.realtor.com/realestateagents/"
    all_data = []
    blocked_ips = []  # List to store blocked IPs
    print("Zip Codes: ", zip_codes)
    proxy_counter = 0  # Counter to track the number of pages processed
    last_processed_zip = None  # Variable to store the last processed zip code
    while zip_codes:  # Iterate until there are zip codes left to process
        # Check if all proxies are blocked
        if len(blocked_ips) == len(proxies):
            logging.error("All IP addresses are blocked. Stopping the process.")
            break  # Exit the loop if all IPs are blocked
        zip_code = zip_codes.pop(0)  # Get and remove the first zip code from the list
        logging.info("Fetching data for zip code: %s", zip_code)
        page_number = 1  # Reset page number for each zip code

        while True:
            page_url = f"{base_url}{zip_code}/pg-{page_number}"  # Corrected URL construction
            logging.info(f"Fetching URL: {page_url}")
            logging.info(f"Scraping data from page {page_number} for zip code {zip_code}...")

            proxy = proxies[proxy_counter % len(proxies)]  # Get the current proxy

            # Parse proxy string to extract username, password, ip address, and port
            proxy_parts = proxy.split(":")
            username = proxy_parts[-2]
            password = proxy_parts[-1]
            ip_address = proxy_parts[0]
            port = proxy_parts[1]

            # Construct proxy URL with username and password
            proxy_url = f"{username}:{password}@{ip_address}:{port}"
            
            last_processed_zip = zip_code  # Update the last processed zip code
            # Check if all proxies are blocked
            if len(blocked_ips) == len(proxies):
                logging.error("All IP addresses are blocked. Stopping the process.")
                break  # Exit the loop if all IPs are blocked

            # If proxy IP is in blocked IPs list, skip this proxy
            if ip_address in blocked_ips:
                logging.info(f"Proxy IP {ip_address} is blocked. Skipping...")
                proxy_counter += 1
                continue
            
            # Extract data from the page
            agent_data, is_blocked = extract_data_from_page(page_url, zip_code, proxy_url)

            # If the request is forbidden (HTTP status code 403), add the proxy IP to the blocked list
            if is_blocked:
                blocked_ips.append(ip_address)
                logging.info(f"Proxy IP {ip_address} is blocked. Adding to blocked IPs list...")
                proxy_counter += 1
                continue

            # If no data is found on the page, break the loop
            if not agent_data:
                logging.info(f"No data found for zip code {zip_code} on page {page_number}. Moving to next zip code...")
                break

            all_data.extend(agent_data)
            page_number += 1
            proxy_counter += 1

            if proxy_counter % 10 == 0:  # Change proxy after every 10 pages
                logging.info("Changing proxy after 10 pages...")
                time.sleep(30)  # Add a delay to avoid rate limiting
            

    df = pd.DataFrame(all_data)
    file_name = "realtor_output_data.xlsx"
    try:
        df.to_excel(file_name, index=False)
        logging.info(f"Data saved to {file_name} file.")
    except Exception as e:
        logging.error(f"Error saving DataFrame to Excel file: {e}")

    if last_processed_zip:
        print(f"Process stopped at zip code: {last_processed_zip}")

if __name__ == "__main__":
    main()