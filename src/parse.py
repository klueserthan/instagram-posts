import jmespath
from typing import Dict

def parse_comment(data: dict) -> dict:
    _stats: dict = jmespath.search(
        """{
        comments_count: edge_media_to_parent_comment.count,
        comments_disabled: comments_disabled,
        comments_next_page: edge_media_to_parent_comment.page_info.end_cursor,
        comments_has_next_page: edge_media_to_parent_comment.page_info.has_next_page
        }""",
        data
    )

    _comments = jmespath.search(
        """{
            comments: edge_media_to_parent_comment.edges[].node.{
                text: text,
                created_at: created_at,
                username: owner.username,
                n_likes: edge_liked_by.count,
                n_replies: edge_threaded_comments.count,
                spam: did_report_as_spam
            }
        }""",
        data
    )

    comments = dict(**_stats)
    comments['comments'] = _comments['comments']

    return comments

def parse_sidecar(data: dict) -> dict:
    
    sidecar: dict = jmespath.search(
        """{
            sidecar: edge_sidecar_to_children.edges[].node
        }""",
        data
    )

    result = [parse_image(image) for image in sidecar['sidecar']]

    return result

def parse_image(data: dict) -> dict:
    print("parsing image")
    result: dict = jmespath.search(
        """{
            shortcode: shortcode,
            url: display_url,
            alt_text: accessibility_caption,
            factcheck_rating: fact_chack_overall_rating,
            factcheck_information: fact_chack_information,
            sensitivitiy_information: sensitivity_friction_info
        }""",
        data
    )

    return result

def parse_video(data: dict) -> dict:
    print("parsing video")
    result: dict = jmespath.search(
        """{
            shortcode: shortcode,
            url: video_url,
            alt_text: accessibility_caption,
            factcheck_rating: fact_chack_overall_rating,
            factcheck_information: fact_chack_information,
            sensitivitiy_information: sensitivity_friction_info,
            video_views: video_view_count,
            viedeo_plays: video_play_count
        }""",
        data
    )

    return result

def parse_post(data: dict) -> dict:
    """Reduce post dataset to the most important fields"""
    print("parsing post")
    result: dict = jmespath.search(
        """{
        id: id,
        shortcode: shortcode,
        post_created: taken_at_timestamp,
        username: owner.username,
        caption: edge_media_to_caption.edges[].node.text,
        n_likes: edge_media_preview_like.count,
        n_comments: edge_media_to_parent_comment.count,
        location: location.name,
        is_video: is_video,
        is_paid_partnership: is_paid_partnership,
        tagged_users: edge_media_to_tagged_user.edges[].node.user.username
    }""",
        data,
    )

    # Comments
    result.update(parse_comment(data))

    # Media
    _type = data.get("__typename")
    if _type == "XDTGraphImage":
        result.update({"images": [parse_image(data)]})
    elif _type == "XDTGraphVideo":
        result.update({"videos": [parse_video(data)]})
    elif _type == "XDTGraphSidecar":
        result.update({"images": parse_sidecar(data)})

    return result