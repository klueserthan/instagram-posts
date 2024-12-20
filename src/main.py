"""This module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""


# HTTPX - A library for making asynchronous HTTP requests in Python. Read more at:
# https://www.python-httpx.org/
from httpx import AsyncClient
from urllib.parse import quote

# JSON - A lightweight data interchange format.
import json

# Apify SDK - A toolkit for building Apify Actors. Read more at:
# https://docs.apify.com/sdk/python
from apify import Actor

# Typing
from typing import Dict

import os

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
async def scrape_post(client: AsyncClient, shortcode: str) -> Dict:
    """Scrape single Instagram post data"""

    variables = quote(json.dumps({
        'shortcode':shortcode,'fetch_tagged_user_count':None,
        'hoisted_comment_id':None,'hoisted_reply_id':None
    }, separators=(',', ':')))
    body = f"variables={variables}&doc_id={INSTAGRAM_DOCUMENT_ID}"
    url = "https://www.instagram.com/graphql/query"

    reponse = await client.post(
        url=url,
        headers={"content-type": "application/x-www-form-urlencoded"},
        data=body
    )
    
    data = json.loads(reponse.content)
    return data["data"]["xdt_shortcode_media"]


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

        # Create an asynchronous HTTPX client for making HTTP requests.
        async with AsyncClient() as client:
            for shortcode in shortcodes: 
                # Fetch the HTML content of the page, following redirects if necessary.
                data = await scrape_post(client, shortcode)

                # Parse
                data = parse_post(data)

                await Actor.push_data(data)



