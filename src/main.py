from httpx import AsyncClient, ReadTimeout
from urllib.parse import quote
import json
import os
import asyncio
from apify import Actor, ProxyConfiguration
from typing import Dict, List
from retry import retry
from datetime import datetime
from src.parse import parse_post
import pdb

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


async def fetch_post_batch_with_proxy(batch: List[str], proxy_configuration: ProxyConfiguration) -> tuple[list[dict], list[str]]:
    """Fetch a batch of shortcodes using a new proxy for each batch."""
    proxy_url = await proxy_configuration.new_url()
    proxies = {'http://': proxy_url, 'https://': proxy_url}
    Actor.log.debug(f"Using proxy: {proxy_url}")

    results = []
    failed = []

    # Async HTTP client with the current proxy
    try:
        async with AsyncClient(proxies=proxies) as client:
            tasks = [scrape_post(client, shortcode) for shortcode in batch]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for shortcode, response in zip(batch, responses):
                if isinstance(response, Exception) or not response:
                    Actor.log.debug(f"Failed to fetch shortcode {shortcode}. Re-adding to retry.")
                    failed.append(shortcode)
                else:
                    results.append(parse_post(response))

    except Exception as e:
        Actor.log.error(f"Error with proxy {proxy_url}: {e}")
        failed.extend(batch)  # Re-add the entire batch if proxy fails

    return results, failed

async def fetch_user_with_proxy(user_id: str, from_date: datetime, proxy_configuration: ProxyConfiguration, page_size: int, max_pages: int, max_retries: int) -> list:
    """
    Fetch posts of a user using Instagram's GraphQL API.

    Args:
        user_id (str): Instagram user ID.
        from_date (datetime): Fetch posts only after this date.
        proxy_configuration (ProxyConfiguration): Proxy configuration object.
        page_size (int): Number of posts to fetch per page.
        max_pages (int): Maximum number of pages to fetch.
        max_retries (int): Maximum number of retries for failed requests per call.

    Returns:
        list: List of parsed posts.
    """
    posts = []
    page_number = 1
    variables = {"id": user_id, "first": page_size, "after": None}
    base_url = "https://www.instagram.com/graphql/query/?query_hash=e769aa130647d2354c40ea6a439bfc08&variables="

    try:
        n_subsequent_errors = 0
        while True:
            proxy_url = await proxy_configuration.new_url()
            proxies = {'http://': proxy_url, 'https://': proxy_url}
            Actor.log.info(f"Using proxy: {proxy_url}")
            try:
                async with AsyncClient(proxies=proxies) as client:
                    response = await client.get(base_url + quote(json.dumps(variables)))

                    if response.status_code != 200:
                        Actor.log.error(f"Failed to fetch user {user_id}. Status code: {response.status_code}")
                        n_subsequent_errors += 1
                        break
                    else:
                        n_subsequent_errors = 0
                    
                    data = response.json()
                    timeline_media = data["data"]["user"]["edge_owner_to_timeline_media"]
                    
                    for edge in timeline_media["edges"]:
                        post = parse_post(edge["node"])

                        # Check date condition
                        if from_date:
                            post_created = datetime.fromtimestamp(post.get("post_created"))
                            if post_created < from_date:
                                Actor.log.info(f"Post date {post_created} older than {from_date}. {len(posts)} shortcodes in queue for user {user_id}. Stopping.")
                                return posts  # Early exit on date condition

                        posts.append(post)

                    page_info = timeline_media["page_info"]
                    if page_number == 1:
                        Actor.log.info(f"Scraping total {timeline_media['count']} posts for user {user_id}")
                    else:
                        Actor.log.info(f"Scraping page {page_number}")

                    # Check pagination conditions
                    if not page_info["has_next_page"]:
                        break
                    if variables["after"] == page_info["end_cursor"]:
                        Actor.log.warning(f"Stuck on the same cursor {variables['after']}. Breaking loop.")
                        break

                    # Update cursor and increment page number
                    variables["after"] = page_info["end_cursor"]
                    page_number += 1

                    if max_pages > 0 and page_number > max_pages:
                        Actor.log.info(f"Reached max pages limit: {max_pages}. Stopping.")
                        break

            except Exception as e:
                Actor.log.error(f"Error with proxy {proxy_url}: {e}")
                if n_subsequent_errors >= max_retries:
                    Actor.log.error(f"Failed 3 times in a row. Stopping.")
                    break
                continue  # Try with a new proxy on failure

    except Exception as final_error:
        Actor.log.error(f"Unhandled error during user fetch: {final_error}")

    return posts

async def fetch_users_with_proxy(user_ids: List[str], from_date: datetime, proxy_configuration: ProxyConfiguration, page_size: int, max_pages: int, concurrency_limit: int, max_retries: int) -> list:
    """
    Fetch posts of multiple users using Instagram's GraphQL API.

    Args:
        user_ids (List[str]): List of Instagram user IDs.
        from_date (datetime): Fetch posts only after this date.
        proxy_configuration (ProxyConfiguration): Proxy configuration object.
        page_size (int): Number of posts to fetch per page.
        max_pages (int, optional): Maximum number of pages to fetch.
        max_retries (int, optional): Maximum number of retries for failed requests per call.

    Returns:
        list: List of parsed posts.
    """
    Actor.log.info(f"Processing {len(user_ids)} users (max_pages: {max_pages}; from_date: {from_date}).")
    semaphore = asyncio.Semaphore(concurrency_limit)
    async with semaphore:
        tasks = [fetch_user_with_proxy(user_id, from_date, proxy_configuration, page_size, max_pages, max_retries) for user_id in user_ids]
        results = await asyncio.gather(*tasks)
    return results

async def fetch_posts_with_proxy(shortcodes: List[str],
                                 proxy_configuration: ProxyConfiguration,
                                 batchsize: int = 10,
                                 concurrency_limit: int = 10,
                                 max_retries: int = 3) -> list:
    Actor.log.info(f"Processing {len(shortcodes)} inputs in batches of size {batchsize}.")
    batches = [shortcodes[i:i + batchsize] for i in range(0, len(shortcodes), batchsize)]
    results = []
    n_failed = len(shortcodes)
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def process_batch(batch):
        async with semaphore:
            batch_results, failed = await fetch_post_batch_with_proxy(batch, proxy_configuration)
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
        Actor.log.info(f"Retrying {len(failed_shortcodes)} failed shortcode(s).")

    for failed_shortcode in failed_shortcodes:
        results.append({"shortcode": failed_shortcode, "error": "NOT_FETCHED", "message": f"Failed to fetch {shortcode} after retries."})
    return results


async def main() -> None:
    """Main entry point for the Apify Actor."""
    os.makedirs(".cache", exist_ok=True)

    async with Actor:
        actor_input = await Actor.get_input()
        shortcodes = actor_input.get("shortcodes", [])
        user_ids = actor_input.get("user_ids", [])
        # Either shortcodes or user_ids must be not empty
        if not shortcodes and not user_ids:
            Actor.log.error("Input must contain 'shortcodes' or 'user_ids'.")
            return

        batchsize = int(actor_input.get("batchsize", 10))
        concurrency_limit = int(actor_input.get("concurrency_limit", 10))
        max_retries = int(actor_input.get("max_retries", 3))
        max_pages = int(actor_input.get("max_pages", 2))

        from_date = str(actor_input.get("from_date"))
        try:
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
            Actor.log.info(f"from_date: {from_date}")
        except ValueError:
            Actor.log.error("Invalid date format for 'from_date'. Expected 'YYYY-MM-DD'.")
            return

        proxy_configuration = await Actor.create_proxy_configuration(
            groups=["RESIDENTIAL"],
            country_code="US",
        )

        results = []
        if user_ids:
            user_results = await fetch_users_with_proxy(user_ids, from_date, proxy_configuration, concurrency_limit=concurrency_limit, max_retries=max_retries, max_pages=max_pages, page_size=12)
            for result in user_results:
                shortcodes.extend([i["shortcode"] for i in result])
        if shortcodes:
            results.extend(await fetch_posts_with_proxy(shortcodes, proxy_configuration, batchsize, concurrency_limit, max_retries))
        
        await Actor.push_data(results)
        Actor.log.info(f"Completed processing. Total results: {len(results)}")
