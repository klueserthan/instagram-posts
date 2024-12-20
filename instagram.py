"""
This is an example web scraper for Instagram.com used in scrapfly blog article:
https://scrapfly.io/blog/how-to-scrape-instagram/

To run this scraper set env variable $SCRAPFLY_KEY with your scrapfly API key:
$ export $SCRAPFLY_KEY="your key from https://scrapfly.io/dashboard"
"""
import json
import os
from typing import Dict, Optional
from urllib.parse import quote
from datetime import datetime
import jmespath
import logging
from scrapfly import ScrapeConfig, ScrapflyClient

logger = logging.getLogger("data_collector" + "." + __name__)

class InstagramScraper:
    BASE_CONFIG = {
        # Instagram.com requires Anti Scraping Protection bypass feature.
        # for more: https://scrapfly.io/docs/scrape-api/anti-scraping-protection
        "asp": True,
        "country": "US",  # change country for relevant results
    }
    INSTAGRAM_APP_ID = "936619743392459"  # this is the public app id for instagram.com
    INSTAGRAM_DOCUMENT_ID = "8845758582119845" # constant id for post documents instagram.com

    def __init__(self):
        self.scrapfly = ScrapflyClient(key=os.environ["SCRAPFLY_KEY"])
        logger.info("scrapfly client initialized")

    @staticmethod
    def parse_user(data: Dict) -> Dict:
        """Reduce the user data to the relevant fields"""
        logger.debug("parsing user data {}", data["username"])
        result = jmespath.search(
            """{
            id: id,
            username: username,
            name: full_name,
            profile_picture_url: profile_pic_url_hd,
            biography: biography,
            category: category_name,
            n_followers: edge_followed_by.count,
            n_follows: edge_follow.count,
            n_posts: edge_owner_to_timeline_media.count,
            is_business_account: is_business_account,
            business_category: business_category_name,
            is_professional_account: is_professional_account,
            is_joined_recently: is_joined_recently,
            is_verified: is_verified,
            is_private: is_private,
            is_regulated_c18: is_regulated_c18,
            
            external_url: external_url,
            related_accounts: edge_related_profiles.edges[].node.username,
            
            bio_links: bio_links[].url,

            video_count: edge_felix_video_timeline.count,
            videos: edge_felix_video_timeline.edges[].node.{
                id: id, 
                title: title,
                shortcode: shortcode,
                thumb: display_url,
                url: video_url,
                views: video_view_count,
                tagged: edge_media_to_tagged_user.edges[].node.user.username,
                captions: edge_media_to_caption.edges[].node.text,
                comments_count: edge_media_to_comment.count,
                comments_disabled: comments_disabled,
                taken_at: taken_at_timestamp,
                likes: edge_liked_by.count,
                location: location.name,
                duration: video_duration
            },
            
            images: edge_felix_video_timeline.edges[].node.{
                id: id, 
                title: title,
                shortcode: shortcode,
                src: display_url,
                url: video_url,
                views: video_view_count,
                tagged: edge_media_to_tagged_user.edges[].node.user.username,
                captions: edge_media_to_caption.edges[].node.text,
                comments_count: edge_media_to_comment.count,
                comments_disabled: comments_disabled,
                taken_at: taken_at_timestamp,
                likes: edge_liked_by.count,
                location: location.name,
                accesibility_caption: accessibility_caption,
                duration: video_duration
            }
        }""",
            data,
        )
        return result


    async def scrape_user(self, username: str) -> Dict:
        """Scrape instagram user's data"""
        logger.info("scraping instagram user {}", username)
        result = await self.scrapfly.async_scrape(
            ScrapeConfig(
                url=f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}",
                headers={"x-ig-app-id": self.INSTAGRAM_APP_ID},
                **self.BASE_CONFIG,
            )
        )
        data = json.loads(result.content)
        return self.parse_user(data["data"]["user"])

    @staticmethod
    def parse_comments(data: Dict) -> Dict:
        """Parse the comments data from the post dataset"""
        if "edge_media_to_comment" in data:
            return jmespath.search(
                """{
                    comments_count: edge_media_to_comment.count,
                    comments_disabled: comments_disabled,
                    comments_next_page: edge_media_to_comment.page_info.end_cursor,
                    comments: edge_media_to_comment.edges[].node.{
                        id: id,
                        text: text,
                        created_at: created_at,
                        owner_id: owner.id,
                        owner: owner.username,
                        owner_verified: owner.is_verified,
                        viewer_has_liked: viewer_has_liked
                    }
                }""",
                data,
            )
        else:
            return jmespath.search(
                """{
                    comments_count: edge_media_to_parent_comment.count,
                    comments_disabled: comments_disabled,
                    comments_next_page: edge_media_to_parent_comment.page_info.end_cursor,
                    comments: edge_media_to_parent_comment.edges[].node.{
                        id: id,
                        text: text,
                        created_at: created_at,
                        owner: owner.username,
                        owner_verified: owner.is_verified,
                        viewer_has_liked: viewer_has_liked,
                        likes: edge_liked_by.count
                    }
                }""",
                data,
            )

    def parse_post(self, data: Dict) -> Dict:
        """Reduce post dataset to the most important fields"""
        logger.debug("parsing post data {}", data["shortcode"])
        result = jmespath.search(
            """{
            id: id,
            shortcode: shortcode,
            dimensions: dimensions,
            src: display_url,
            thumbnail_src: thumbnail_src,
            media_preview: media_preview,
            video_url: video_url,
            views: video_view_count,
            likes: edge_media_preview_like.count,
            location: location.name,
            taken_at: taken_at_timestamp,
            related: edge_web_media_to_related_media.edges[].node.shortcode,
            type: product_type,
            video_duration: video_duration,
            music: clips_music_attribution_info,
            is_video: is_video,
            tagged_users: edge_media_to_tagged_user.edges[].node.user.username,
            captions: edge_media_to_caption.edges[].node.text,
            related_profiles: edge_related_profiles.edges[].node.username
        }""",
            data,
        )
        comments_data = self.parse_comments(data)
        result.update(comments_data)

        return result


    async def scrape_post(self, url_or_shortcode: str) -> Dict:
        """Scrape single Instagram post data"""
        if "http" in url_or_shortcode:
            shortcode = url_or_shortcode.split("/p/")[-1].split("/")[0]
        else:
            shortcode = url_or_shortcode
        logger.info("scraping instagram post: {}", shortcode)
        variables = quote(json.dumps({
            'shortcode':shortcode,'fetch_tagged_user_count':None,
            'hoisted_comment_id':None,'hoisted_reply_id':None
        }, separators=(',', ':')))
        body = f"variables={variables}&doc_id={self.INSTAGRAM_DOCUMENT_ID}"
        url = "https://www.instagram.com/graphql/query"
        result = await self.scrapfly.async_scrape(
            ScrapeConfig(
                url=url,
                method="POST",
                body=body,
                headers={"content-type": "application/x-www-form-urlencoded"},
                **self.BASE_CONFIG
            )
        )
        
        data = json.loads(result.content)
        return self.parse_post(data["data"]["xdt_shortcode_media"])


    async def scrape_user_posts(self, user_id: str, page_size=24, max_pages: Optional[int] = None, date_from: Optional[datetime] = None):
        """Scrape all posts of an instagram user of given numeric user id"""
        base_url = "https://www.instagram.com/graphql/query/?query_hash=e769aa130647d2354c40ea6a439bfc08&variables="
        variables = {
            "id": user_id,
            "first": page_size,
            "after": None,
        }
        _page_number = 1
        while True:
            url = base_url + quote(json.dumps(variables))
            result = await self.scrapfly.async_scrape(ScrapeConfig(url, **self.BASE_CONFIG))
            data = json.loads(result.content)
            posts = data["data"]["user"]["edge_owner_to_timeline_media"]
            for post in posts["edges"]:
                yield self.parse_post(post["node"])
            page_info = posts["page_info"]
            if _page_number == 1:
                logger.info(f"scraping total {posts['count']} posts of {user_id}")
            else:
                logger.info(f"scraping posts page {_page_number}")
            if not page_info["has_next_page"]:
                break
            if variables["after"] == page_info["end_cursor"]:
                break
            variables["after"] = page_info["end_cursor"]
            _page_number += 1
            if max_pages and _page_number > max_pages:
                break