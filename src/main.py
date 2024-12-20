"""This module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""


# HTTPX - A library for making asynchronous HTTP requests in Python. Read more at:
# https://www.python-httpx.org/
from httpx import AsyncClient, ReadTimeout
from urllib.parse import quote

# JSON - A lightweight data interchange format.
import json

# Apify SDK - A toolkit for building Apify Actors. Read more at:
# https://docs.apify.com/sdk/python
from apify import Actor

# Typing
from typing import Dict

import os

from retry import retry

from src.parse import parse_post


# Constants
BASE_CONFIG = {
    # Instagram.com requires Anti Scraping Protection bypass feature.
    # for more: https://scrapfly.io/docs/scrape-api/anti-scraping-protection
    "asp": True,
    "country": "US",  # change country for relevant results
}
INSTAGRAM_APP_ID = "936619743392459"  # this is the public app id for instagram.com
INSTAGRAM_DOCUMENT_ID = "8845758582119845" # constant id for post documents instagram.com

# Functions
@retry(ReadTimeout, tries=3, delay=10, backoff=3, jitter=2, logger=Actor.log)
async def scrape_post(client: AsyncClient, shortcode: str) -> Dict:
    """Scrape single Instagram post data"""

    variables = quote(json.dumps({
        'shortcode':shortcode,'fetch_tagged_user_count':None,
        'hoisted_comment_id':None,'hoisted_reply_id':None
    }, separators=(',', ':')))
    body = f"variables={variables}&doc_id={INSTAGRAM_DOCUMENT_ID}"
    url = "https://www.instagram.com/graphql/query"

    response = await client.post(
        url=url,
        headers={"content-type": "application/x-www-form-urlencoded"},
        data=body
    )

    # Check response
    if response.status_code == 401:
        Actor.log.error("Unauthorized.")
        return None
    elif not response.status_code == 200:
        Actor.log.error(f"Failed to fetch {shortcode}.")
        return None

    data = json.loads(response.content)
    try: 
        data = data["data"]["xdt_shortcode_media"]
    except KeyError:
        Actor.log.error(f"Failed to fetch xdt_shortcode_media for {shortcode}.")
        return None
    
    return data


async def main() -> None:
    """Main entry point for the Apify Actor.

    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """

    # Check there is a .cache directory
    if not os.path.exists(".cache"):
        os.mkdir(".cache")

    async with Actor:
        actor_input = await Actor.get_input()
        shortcodes = actor_input.get('shortcodes')
        Actor.log.info(f"Processing {len(shortcodes)} inputs")

        # Set up the proxy configuration
        proxy_configuration = await Actor.create_proxy_configuration(
            groups = ["RESIDENTIAL"],
            country_code= "US",
        )

        # Create an asynchronous HTTPX client for making HTTP requests.
        Actor.log.info(f"Using proxy URL: {proxy_configuration.new_url()}")
        async with AsyncClient() as client:
            for shortcode in shortcodes: 
                # Fetch the HTML content of the page, following redirects if necessary.
                data = await scrape_post(client, shortcode)

                # Parse
                if not data:
                    continue
                data = parse_post(data)

                await Actor.push_data(data)



