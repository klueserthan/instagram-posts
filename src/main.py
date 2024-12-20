from httpx import AsyncClient, ReadTimeout
from urllib.parse import quote
import json
import os
import asyncio
from apify import Actor, ProxyConfiguration
from typing import Dict, List
from retry import retry
from src.parse import parse_post

INSTAGRAM_DOCUMENT_ID = "8845758582119845"

@retry(ReadTimeout, tries=3, delay=2, backoff=2, logger=Actor.log)
async def scrape_post(client: AsyncClient, shortcode: str) -> Dict:
    """Scrape single Instagram post data."""
    variables = json.dumps({
        'shortcode': shortcode, 
        'fetch_tagged_user_count': None,
        'hoisted_comment_id': None, 
        'hoisted_reply_id': None
    }, separators=(',', ':'))

    body = f"variables={quote(variables)}&doc_id={INSTAGRAM_DOCUMENT_ID}"
    url = "https://www.instagram.com/graphql/query"
    response = await client.post(
        url=url,
        headers={"content-type": "application/x-www-form-urlencoded"},
        data=body
    )

    if response.status_code != 200:
        Actor.log.error(f"Failed to fetch {shortcode}. Status code: {response.status_code}")
        return None

    try:
        data = json.loads(response.content)["data"]["xdt_shortcode_media"]
        return data
    except KeyError:
        Actor.log.error(f"Invalid response format for shortcode {shortcode}.")
        return None


async def fetch_batch_with_proxy(batch: List[str], proxy_configuration: ProxyConfiguration) -> tuple[list[dict], list[str]]:
    """Fetch a batch of shortcodes using a new proxy for each batch."""
    proxy_url = await proxy_configuration.new_url()
    proxies = {'http://': proxy_url, 'https://': proxy_url}
    #Actor.log.info(f"Using proxy: {proxy_url}")

    results = []
    failed = []

    # Async HTTP client with the current proxy
    try:
        async with AsyncClient(proxies=proxies) as client:
            tasks = [scrape_post(client, shortcode) for shortcode in batch]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for shortcode, response in zip(batch, responses):
                if isinstance(response, Exception) or not response:
                    #Actor.log.error(f"Failed to fetch shortcode {shortcode}. Re-adding to retry.")
                    failed.append(shortcode)
                else:
                    results.append(parse_post(response))

    except Exception as e:
        Actor.log.error(f"Error with proxy {proxy_url}: {e}")
        failed.extend(batch)  # Re-add the entire batch if proxy fails

    return results, failed


async def main() -> None:
    """Main entry point for the Apify Actor."""
    os.makedirs(".cache", exist_ok=True)

    async with Actor:
        actor_input = await Actor.get_input()
        shortcodes = actor_input.get("shortcodes", [])
        batchsize = actor_input.get("batchsize", 10)
        concurrency_limit = actor_input.get("concurrency_limit", 10)
        max_retries = actor_input.get("max_retries", 3)
        Actor.log.info(f"Processing {len(shortcodes)} inputs in batches of size {batchsize}.")

        proxy_configuration = await Actor.create_proxy_configuration(
            groups=["RESIDENTIAL"],
            country_code="US",
        )

        batches = [shortcodes[i:i + batchsize] for i in range(0, len(shortcodes), batchsize)]
        results = []
        retries = 0
        n_failed = len(shortcodes)
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def process_batch(batch):
            async with semaphore:
                batch_results, failed = await fetch_batch_with_proxy(batch, proxy_configuration)
                if batch_results:
                    results.extend(batch_results)
                return failed

        no_progress = 0
        while batches:
            tasks = [process_batch(batch) for batch in batches]
            failed_batches = await asyncio.gather(*tasks)

            # Flatten the list of failed batches and re-add them
            failed_shortcodes = []
            for failed_batch in failed_batches:
                for shortcode in failed_batch:
                    failed_shortcodes.append(shortcode)

            # Only continue if n_failed is decreasing
            if len(failed_shortcodes) >= n_failed:
                no_progress += 1
                if no_progress == max_retries:
                    Actor.log.error("No progress after {max_retries}. Stopping.")
                    break
            else:
                no_progress = 0

            n_failed = len(failed_shortcodes)
            batches = [failed_shortcodes[i:i + batchsize] for i in range(0, len(failed_shortcodes), batchsize)]
            Actor.log.info(f"Retrying {len(failed_shortcodes)} failed shortcodes.")

        await Actor.push_data(results)
        Actor.log.info(f"Completed processing. Total results: {len(results)}")
