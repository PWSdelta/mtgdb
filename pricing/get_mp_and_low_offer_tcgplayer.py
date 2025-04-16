from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import random
import json


def scrape_tcg_listings(url, offer_prices_selector, market_price_selector, most_recent_sale_selector, max_listings=10):
    """
    A simplified function to scrape TCGPlayer listings

    Args:
        url (str): URL of the TCGPlayer product page
        offer_prices_selector (str): CSS selector for the listing prices
        market_price_selector (str): CSS selector for the market price element
        most_recent_sale_selector (str): CSS selector for the most recent sale element
        max_listings (int): Maximum number of listings to extract (default: 10)
    """
    # Configure Chrome options
    chrome_options = Options()
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Initialize the WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1366, 768)

    try:
        # Hide WebDriver usage
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })

        print(f"Navigating to {url}...")
        driver.get(url)

        # Wait for page to load
        time.sleep(3)

        # Simple scrolling to make sure content is loaded
        driver.execute_script("window.scrollBy(0, 300);")
        time.sleep(1)

        # Scroll down in stages
        for i in range(3):
            driver.execute_script(f"window.scrollBy(0, 500);")
            time.sleep(1)

        # Get the page source
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract data
        results = {
            'market_price': None,
            'most_recent_sale': None,
            'listings': []
        }

        # Get market price
        market_price_element = soup.select_one(market_price_selector)
        if market_price_element:
            results['market_price'] = {
                'text': market_price_element.get_text(strip=True),
                'html': str(market_price_element)
            }
            print(f"Found market price: {results['market_price']['text']}")

        # Get most recent sale
        most_recent_sale_element = soup.select_one(most_recent_sale_selector)
        if most_recent_sale_element:
            results['most_recent_sale'] = {
                'text': most_recent_sale_element.get_text(strip=True),
                'html': str(most_recent_sale_element)
            }
            print(f"Found most recent sale: {results['most_recent_sale']['text']}")

        # Get listings
        for i in range(1, max_listings + 1):
            # Modify selector for each listing
            current_selector = offer_prices_selector.replace('nth-child(INDEX)', f'nth-child({i})')
            element = soup.select_one(current_selector)

            if element:
                listing_data = {
                    'index': i,
                    'text': element.get_text(strip=True),
                    'html': str(element)
                }
                results['listings'].append(listing_data)
                print(f"Found listing {i}: {listing_data['text']}")
            else:
                print(f"Listing {i} not found.")

        print(f"\nTotal listings found: {len(results['listings'])}")

        # Save results
        with open('scraped_data.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print("Data saved to 'scraped_data.json'")
        return results

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        # Take a screenshot for debugging
        try:
            driver.save_screenshot("page_screenshot.png")
            print("Screenshot saved as 'page_screenshot.png'")
        except:
            pass

        # Close the browser
        driver.quit()


# Main execution
if __name__ == "__main__":
    target_url = "https://www.tcgplayer.com/product/8989/magic-unlimited-edition-black-lotus?Language=English"
    market_price_selector = "#app > div > div > section.marketplace__content > section > div.product-details-container > div.product-details__product > section.product-details__price-guide > div > section.price-guide__points > div > div.price-points__upper > table:nth-child(1) > tr.price-points__upper__header > td:nth-child(2) > span"
    offer_prices_selector = "#app > div > div > section.marketplace__content > section > section.product-details__listings > section > section > section > div.product-details__listings-results > section:nth-child(INDEX) > div > div.listing-item__listing-data__info > div.listing-item__listing-data__info__price"
    most_recent_sale_selector = "#app > div > div > section.marketplace__content > section > div.product-details-container > div.product-details__product > section.product-details__price-guide > div > section.price-guide__points > div > div.price-points__upper > table > tr:nth-child(2) > td:nth-child(2) > span"

    result = scrape_tcg_listings(
        target_url,
        offer_prices_selector,
        market_price_selector,
        most_recent_sale_selector,
        max_listings=10  # Just get 10 listings
    )

    # Display the results
    if result and 'listings' in result and result['listings']:
        print(f"\nMarket Price: {result['market_price']['text'] if result['market_price'] else 'Not found'}")
        print(f"Most Recent Sale: {result['most_recent_sale']['text'] if result['most_recent_sale'] else 'Not found'}")
        print("\nListings Found:")
        for i, listing in enumerate(result['listings']):
            print(f"{i + 1}. {listing['text']}")
    else:
        print("No results found or scraping failed.")