from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import time
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Website and ChromeDriver Configurations
webpage_url = 'https://tcgcsv.com/#daily-updates'
chromedriver_path = r"C:\Users\Owner\Downloads\chromedriver-win64\chromedriver.exe"

# Desired game IDs
# desired_game_ids = {i: None for i in range(1, 87)}
# desired_game_ids = { 1, 3, 68, 71 }
desired_game_ids = { 1 }
# desired_game_ids = { 1, 3, 7, 9, 10, 13, 16, 17, 19, 20, 21, 23, 24, 25, 37, 38, 53, 61, 62, 63, 64, 65, 66, 68, 74, 75, 76, 77, 78, 79, 80, 81, 83, 84, 85, 86 }

# Directory to save downloaded files
output_directory = "downloads/"
os.makedirs(output_directory, exist_ok=True)  # Create the directory if it doesn't exist


def get_filtered_csv_links(url, chromedriver_path, game_ids):
    """
    Use Selenium to scrape and filter .csv links for the given game IDs.
    """
    # Base URL for handling relative links
    base_url = "https://tcgcsv.com/"

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Load the webpage
        driver.get(url)

        # Wait for the page to fully load
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Scroll the page to ensure all dynamic content is loaded
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

        # Get the fully rendered HTML content
        html_content = driver.page_source

        # Use BeautifulSoup to extract all links
        soup = BeautifulSoup(html_content, 'html.parser')
        all_links = [link.get('href') for link in soup.find_all('a', href=True)]

        # Debug: Print all extracted links (optional for debugging purposes)
        print("All Links Found on Page:")
        for link in all_links:
            print(link)

        # Regex to match .csv links
        csv_pattern = r'(?:https?://)?tcgplayer/\d+/\d+/ProductsAndPrices\.csv'

        # Filter links that match the .csv pattern
        csv_links = [link for link in all_links if re.match(csv_pattern, link)]

        # Ensure all links are absolute URLs
        csv_links = [link if link.startswith("http") else base_url + link for link in csv_links]

        # Filter links for the desired game IDs
        filtered_links = []
        for link in csv_links:
            match = re.search(r'/tcgplayer/(\d+)/', link)
            if match and int(match.group(1)) in game_ids:
                filtered_links.append(link)

        print(f"Found {len(filtered_links)} CSV links for the desired game IDs: {game_ids}")
        return filtered_links

    finally:
        driver.quit()


def extract_game_and_group_from_link(link):
    """
    Extract the game ID and group ID from a .csv link.
    """
    match = re.search(r'/tcgplayer/(\d+)/(\d+)/ProductsAndPrices\.csv', link)
    if match:
        game_id = match.group(1)
        group_id = match.group(2)
        return game_id, group_id
    return None, None


def download_file(link, output_directory):
    """
    Download a single .csv file from the given link and save it to the output directory.
    """
    try:
        # Extract game ID and group ID for unique naming
        game_id, group_id = extract_game_and_group_from_link(link)
        if game_id and group_id:
            filename = f"ProductsAndPrices_game_{game_id}_group_{group_id}.csv"
        else:
            # Default fallback filename
            filename = f"ProductsAndPrices.csv"

        # Send the GET request
        response = requests.get(link, timeout=30)
        if response.status_code == 200:
            # Save the file locally
            filepath = os.path.join(output_directory, filename)
            with open(filepath, "wb") as file:
                file.write(response.content)

            print(f"Successfully saved: {filepath}")
            return filepath
        else:
            print(f"Failed to download {link} (HTTP {response.status_code})")
            return None
    except Exception as e:
        print(f"Error downloading {link}: {e}")
        return None


def download_csv_files_concurrently(csv_links, output_directory, max_workers=10):
    """
    Downloads multiple .csv files concurrently using a thread pool.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a future for each file download
        future_to_link = {executor.submit(download_file, link, output_directory): link for link in csv_links}

        # Process each completed download
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                result = future.result()  # This will raise any exceptions that occurred
                if result:
                    print(f"Download completed: {result}")
                else:
                    print(f"Download failed: {link}")
            except Exception as e:
                print(f"Error during download of {link}: {e}")


# Main Execution
if __name__ == '__main__':
    print(f"Fetching CSV links for games {desired_game_ids} using Selenium...")

    # Get filtered .csv links
    filtered_csv_links = get_filtered_csv_links(webpage_url, chromedriver_path, desired_game_ids)

    if filtered_csv_links:
        print("\nFiltered List of CSV Links:")
        for link in filtered_csv_links:
            print(link)

        # Download the CSV files concurrently
        print("\nDownloading CSV files concurrently...")
        download_csv_files_concurrently(filtered_csv_links, output_directory, max_workers=6)
    else:
        print("No relevant CSV links found.")