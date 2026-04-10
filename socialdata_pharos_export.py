#!/usr/bin/env python3
"""Export and refresh X/Twitter posts matching @pharos_network via SocialData API."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


API_BASE_URL = "https://api.socialdata.tools/twitter/search"
API_COMMUNITY_URL_TEMPLATE = "https://api.socialdata.tools/twitter/community/{community_id}/tweets"
DEFAULT_QUERY = "@pharos_network"
DEFAULT_TIMEOUT = 30
DEFAULT_SINCE_DATE = "2026-04-07"
DEFAULT_USERS_FILE = "users_summary.json"
DEFAULT_TWEETS_FILE = "tweets.json"
DEFAULT_REFRESH_COUNT = 50
DEFAULT_COMMUNITY_ID = "1944638720218734921"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect tweets from SocialData search and export two JSON files: "
            "per-user summary and per-tweet details."
        )
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("SOCIALDATA_API_KEY"),
        help="SocialData API key. Defaults to SOCIALDATA_API_KEY env var.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "update"),
        default="full",
        help="full = rebuild all data from 2026-04-07, update = fetch new tweets and refresh latest saved ones.",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help="Search query text. Default: %(default)s",
    )
    parser.add_argument(
        "--community-id",
        default=DEFAULT_COMMUNITY_ID,
        help="Community ID to additionally parse. Default: %(default)s",
    )
    parser.add_argument(
        "--no-community",
        action="store_true",
        help="Disable community parsing and use only the search source.",
    )
    parser.add_argument(
        "--since-date",
        default=DEFAULT_SINCE_DATE,
        help="Start date in YYYY-MM-DD format. Default: %(default)s",
    )
    parser.add_argument(
        "--until-date",
        help=(
            "Optional inclusive end date in YYYY-MM-DD format. "
            "If omitted, search runs until the latest available results."
        ),
    )
    parser.add_argument(
        "--users-file",
        default=DEFAULT_USERS_FILE,
        help="Summary JSON filename in project root. Default: %(default)s",
    )
    parser.add_argument(
        "--tweets-file",
        default=DEFAULT_TWEETS_FILE,
        help="Tweets JSON filename in project root. Default: %(default)s",
    )
    parser.add_argument(
        "--refresh-count",
        type=int,
        default=DEFAULT_REFRESH_COUNT,
        help="In update mode, refresh this many most recent saved tweets. Default: %(default)s",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.25,
        help="Delay between paginated requests in seconds. Default: %(default)s",
    )
    return parser.parse_args()


def parse_date_to_timestamp(value: str, *, end_of_day: bool) -> int:
    try:
        date_value = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc

    if end_of_day:
        dt = date_value.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    else:
        dt = date_value.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    return int(dt.timestamp())


def parse_twitter_datetime(value: str) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    try:
        # Supports stored values like 2026-04-10T03:56:33+00:00
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        pass

    formats = (
        "%a %b %d %H:%M:%S %z %Y",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def timestamp_from_tweet(tweet: dict[str, Any]) -> int | None:
    raw_value = str(tweet.get("tweet_created_at") or tweet.get("created_at") or "")
    parsed = parse_twitter_datetime(raw_value)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def build_search_query(base_query: str, since_date: str, until_date: str | None) -> str:
    parts = [base_query.strip(), f"since_time:{parse_date_to_timestamp(since_date, end_of_day=False)}"]
    if until_date:
        parts.append(f"until_time:{parse_date_to_timestamp(until_date, end_of_day=True)}")
    return " ".join(part for part in parts if part)


def build_incremental_query(
    base_query: str,
    *,
    since_date: str,
    last_posted_at: str | None,
    until_date: str | None,
) -> str:
    parts = [base_query.strip()]
    if last_posted_at:
        dt = parse_twitter_datetime(last_posted_at)
        if dt is not None:
            parts.append(f"since_time:{int(dt.timestamp())}")
        else:
            fallback_ts = parse_date_to_timestamp(since_date, end_of_day=False)
            print(
                f"Warning: failed to parse last_posted_at='{last_posted_at}', fallback since_time:{fallback_ts}",
                file=sys.stderr,
            )
            parts.append(f"since_time:{fallback_ts}")
    else:
        parts.append(f"since_time:{parse_date_to_timestamp(since_date, end_of_day=False)}")

    if until_date:
        parts.append(f"until_time:{parse_date_to_timestamp(until_date, end_of_day=True)}")
    return " ".join(parts)


def get_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }


def request_search_page(
    session: requests.Session,
    *,
    query: str,
    cursor: str | None,
) -> dict[str, Any]:
    params = {
        "query": query,
        "type": "Latest",
    }
    if cursor:
        params["cursor"] = cursor

    response = session.get(API_BASE_URL, params=params, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected API response shape: root payload is not an object.")
    return payload


def request_community_page(
    session: requests.Session,
    *,
    community_id: str,
    cursor: str | None,
) -> dict[str, Any]:
    params = {"type": "Latest"}
    if cursor:
        params["cursor"] = cursor

    url = API_COMMUNITY_URL_TEMPLATE.format(community_id=community_id)
    response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected API response shape: root payload is not an object.")
    return payload


def pick_text(tweet: dict[str, Any]) -> str:
    return str(tweet.get("full_text") or tweet.get("text") or "")


def is_quote_tweet(tweet: dict[str, Any]) -> bool:
    return bool(
        tweet.get("is_quote_status")
        or tweet.get("quoted_status_id")
        or tweet.get("quoted_status_id_str")
        or tweet.get("quoted_status")
    )


def is_reply_tweet(tweet: dict[str, Any]) -> bool:
    return bool(tweet.get("in_reply_to_status_id") or tweet.get("in_reply_to_status_id_str"))


def should_skip_search_tweet(tweet: dict[str, Any]) -> bool:
    return is_reply_tweet(tweet) and not is_quote_tweet(tweet)


def should_skip_community_tweet(tweet: dict[str, Any]) -> bool:
    return is_reply_tweet(tweet)


def profile_image_url(user: dict[str, Any]) -> str:
    raw = str(user.get("profile_image_url_https") or user.get("profile_image_url") or "")
    return raw.replace("_normal", "") if raw else ""


def build_tweet_url(tagname: str, tweet_id: str) -> str:
    return f"https://x.com/{tagname}/status/{tweet_id}"


def build_profile_url(tagname: str) -> str:
    return f"https://x.com/{tagname}"


def extract_media_url(tweet: dict[str, Any]) -> str | None:
    containers = []
    for key in ("extended_entities", "entities"):
        value = tweet.get(key)
        if isinstance(value, dict):
            containers.append(value)

    for container in containers:
        media = container.get("media")
        if not isinstance(media, list):
            continue
        for item in media:
            if not isinstance(item, dict):
                continue
            media_type = item.get("type")
            if media_type and media_type != "photo":
                continue
            media_url = item.get("media_url_https") or item.get("media_url")
            if media_url:
                return str(media_url)
    return None


def extract_posted_at(tweet: dict[str, Any]) -> tuple[str, str]:
    raw_value = str(tweet.get("tweet_created_at") or tweet.get("created_at") or "")
    parsed = parse_twitter_datetime(raw_value)
    if parsed is None:
        return "", raw_value
    return parsed.isoformat(), parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def tweet_to_row(tweet: dict[str, Any]) -> dict[str, Any] | None:
    if should_skip_search_tweet(tweet):
        return None

    user = tweet.get("user")
    if not isinstance(user, dict):
        return None

    tagname = str(user.get("screen_name") or "").strip()
    if not tagname:
        return None

    tweet_id = str(tweet.get("id_str") or tweet.get("id") or "").strip()
    posted_at_iso, posted_at = extract_posted_at(tweet)
    likes = int(tweet.get("favorite_count") or 0)
    replies = int(tweet.get("reply_count") or 0)
    reposts = int(tweet.get("retweet_count") or 0) + int(tweet.get("quote_count") or 0)
    views = int(tweet.get("views_count") or 0)

    return {
        "tweet_id": tweet_id,
        "name": str(user.get("name") or "").strip(),
        "tagname": tagname,
        "link": build_profile_url(tagname),
        "text": pick_text(tweet),
        "pic": extract_media_url(tweet),
        "like": likes,
        "reply": replies,
        "repost": reposts,
        "views": views,
        "posted_at": posted_at,
        "posted_at_iso": posted_at_iso,
        "is_quote": is_quote_tweet(tweet),
        "tweet_url": build_tweet_url(tagname, tweet_id) if tweet_id else "",
        "pfp": profile_image_url(user),
    }


def normalize_loaded_tweet(tweet: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(tweet)
    normalized["tweet_id"] = str(tweet.get("tweet_id") or "")
    normalized["name"] = str(tweet.get("name") or "")
    normalized["tagname"] = str(tweet.get("tagname") or "")
    normalized["link"] = str(tweet.get("link") or build_profile_url(normalized["tagname"]))
    normalized["text"] = str(tweet.get("text") or "")
    normalized["pic"] = tweet.get("pic")
    normalized["like"] = int(tweet.get("like") or 0)
    normalized["reply"] = int(tweet.get("reply") or 0)
    normalized["repost"] = int(tweet.get("repost") or 0)
    normalized["views"] = int(tweet.get("views") or 0)
    normalized["posted_at"] = str(tweet.get("posted_at") or "")
    normalized["posted_at_iso"] = str(tweet.get("posted_at_iso") or "")
    normalized["is_quote"] = bool(tweet.get("is_quote") or False)
    normalized["tweet_url"] = str(tweet.get("tweet_url") or "")
    normalized["pfp"] = str(tweet.get("pfp") or "")
    return normalized


def sort_tweets(tweets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        tweets,
        key=lambda item: (
            item.get("posted_at_iso") or "",
            item.get("tweet_id") or "",
        ),
        reverse=True,
    )


def summarize_users(tweet_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    user_stats: dict[str, dict[str, Any]] = {}

    for tweet in tweet_rows:
        tagname = str(tweet.get("tagname") or "").strip()
        if not tagname:
            continue

        if tagname not in user_stats:
            user_stats[tagname] = {
                "name": str(tweet.get("name") or "").strip(),
                "tagname": tagname,
                "pfp": str(tweet.get("pfp") or "").strip(),
                "post": 0,
                "like": 0,
                "reply": 0,
                "repost": 0,
                "views": 0,
            }

        stats = user_stats[tagname]
        if not stats["name"] and tweet.get("name"):
            stats["name"] = str(tweet["name"]).strip()
        if not stats["pfp"] and tweet.get("pfp"):
            stats["pfp"] = str(tweet["pfp"]).strip()

        stats["post"] += 1
        stats["like"] += int(tweet.get("like") or 0)
        stats["reply"] += int(tweet.get("reply") or 0)
        stats["repost"] += int(tweet.get("repost") or 0)
        stats["views"] += int(tweet.get("views") or 0)

    return sorted(
        user_stats.values(),
        key=lambda item: (-item["post"], -item["views"], item["tagname"].lower()),
    )


def fetch_all_tweets(
    session: requests.Session,
    *,
    query: str,
    request_delay: float,
    source_label: str,
) -> list[dict[str, Any]]:
    all_tweets: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    cursor: str | None = None
    page = 0

    while True:
        page += 1
        payload = request_search_page(session, query=query, cursor=cursor)
        tweets = payload.get("tweets")
        if not isinstance(tweets, list):
            raise RuntimeError("Unexpected API response shape: 'tweets' is not an array.")

        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            if should_skip_search_tweet(tweet):
                continue
            tweet_id = str(tweet.get("id_str") or tweet.get("id") or "")
            if tweet_id and tweet_id in seen_ids:
                continue
            if tweet_id:
                seen_ids.add(tweet_id)
            all_tweets.append(tweet)

        print(f"[{source_label} page {page}] total posts collected: {len(all_tweets)}", file=sys.stderr)

        next_cursor = payload.get("next_cursor")
        if not next_cursor or not isinstance(next_cursor, str):
            break
        if not tweets:
            break
        cursor = next_cursor
        if request_delay > 0:
            time.sleep(request_delay)

    return all_tweets


def fetch_community_tweets(
    session: requests.Session,
    *,
    community_id: str,
    request_delay: float,
    since_timestamp: int,
    until_timestamp: int | None,
) -> list[dict[str, Any]]:
    all_tweets: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    cursor: str | None = None
    page = 0

    while True:
        page += 1
        payload = request_community_page(session, community_id=community_id, cursor=cursor)
        tweets = payload.get("tweets")
        if not isinstance(tweets, list):
            raise RuntimeError("Unexpected API response shape: 'tweets' is not an array.")

        page_kept = 0
        page_has_relevant_window = False
        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            if should_skip_community_tweet(tweet):
                continue

            tweet_ts = timestamp_from_tweet(tweet)
            if tweet_ts is None:
                continue
            if tweet_ts < since_timestamp:
                continue
            if until_timestamp is not None and tweet_ts > until_timestamp:
                continue

            page_has_relevant_window = True
            tweet_id = str(tweet.get("id_str") or tweet.get("id") or "")
            if tweet_id and tweet_id in seen_ids:
                continue
            if tweet_id:
                seen_ids.add(tweet_id)
            all_tweets.append(tweet)
            page_kept += 1

        print(
            f"[community page {page}] total posts collected: {len(all_tweets)}",
            file=sys.stderr,
        )

        if not tweets:
            break

        oldest_timestamp = min((timestamp_from_tweet(item) or 0) for item in tweets if isinstance(item, dict))
        next_cursor = payload.get("next_cursor")
        if not next_cursor or not isinstance(next_cursor, str):
            break
        if oldest_timestamp and oldest_timestamp < since_timestamp and not page_has_relevant_window:
            break

        cursor = next_cursor
        if request_delay > 0:
            time.sleep(request_delay)

    return all_tweets


def load_existing_tweets(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"Existing tweets file has invalid format: {path}")
    return [normalize_loaded_tweet(item) for item in payload if isinstance(item, dict)]


def write_json(path: Path, data: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def build_lookup(tweets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for tweet in tweets:
        tweet_id = str(tweet.get("tweet_id") or "")
        if tweet_id:
            lookup[tweet_id] = tweet
    return lookup


def merge_tweets(
    existing_tweets: list[dict[str, Any]],
    fetched_tweets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = build_lookup(existing_tweets)

    for tweet in fetched_tweets:
        row = tweet_to_row(tweet)
        if row is None:
            continue
        tweet_id = row["tweet_id"]
        if not tweet_id:
            continue
        merged[tweet_id] = row

    return sort_tweets(list(merged.values()))


def pick_refresh_seed(existing_tweets: list[dict[str, Any]], refresh_count: int) -> str | None:
    if refresh_count <= 0 or not existing_tweets:
        return None
    recent_slice = sort_tweets(existing_tweets)[:refresh_count]
    if not recent_slice:
        return None
    oldest_in_slice = recent_slice[-1]
    return str(oldest_in_slice.get("posted_at_iso") or "")


def run_full_mode(
    session: requests.Session,
    *,
    query: str,
    community_id: str | None,
    since_date: str,
    until_date: str | None,
    request_delay: float,
) -> list[dict[str, Any]]:
    final_query = build_search_query(query, since_date, until_date)
    since_timestamp = parse_date_to_timestamp(since_date, end_of_day=False)
    until_timestamp = parse_date_to_timestamp(until_date, end_of_day=True) if until_date else None

    print(f"Mode: full | search query: {final_query}", file=sys.stderr)
    search_tweets = fetch_all_tweets(
        session,
        query=final_query,
        request_delay=request_delay,
        source_label="search",
    )
    merged = merge_tweets([], search_tweets)

    if community_id:
        print(f"Mode: full | community id: {community_id}", file=sys.stderr)
        community_tweets = fetch_community_tweets(
            session,
            community_id=community_id,
            request_delay=request_delay,
            since_timestamp=since_timestamp,
            until_timestamp=until_timestamp,
        )
        merged = merge_tweets(merged, community_tweets)

    print(f"Full rebuild complete. Total posts collected: {len(merged)}", file=sys.stderr)
    return merged


def run_update_mode(
    session: requests.Session,
    *,
    query: str,
    community_id: str | None,
    since_date: str,
    until_date: str | None,
    request_delay: float,
    existing_tweets: list[dict[str, Any]],
    refresh_count: int,
) -> list[dict[str, Any]]:
    if not existing_tweets:
        print("Existing tweets.json not found or empty. Falling back to full mode.", file=sys.stderr)
        return run_full_mode(
            session,
            query=query,
            community_id=community_id,
            since_date=since_date,
            until_date=until_date,
            request_delay=request_delay,
        )

    latest_saved = sort_tweets(existing_tweets)
    newest_posted_at = str(latest_saved[0].get("posted_at_iso") or "")
    refresh_seed = pick_refresh_seed(latest_saved, refresh_count)
    incremental_query = build_incremental_query(
        query,
        since_date=since_date,
        last_posted_at=refresh_seed or newest_posted_at,
        until_date=until_date,
    )

    print(
        f"Mode: update | refreshing last {min(refresh_count, len(existing_tweets))} posts "
        f"and fetching new ones from {refresh_seed or newest_posted_at}",
        file=sys.stderr,
    )
    print(f"Update query: {incremental_query}", file=sys.stderr)

    search_tweets = fetch_all_tweets(
        session,
        query=incremental_query,
        request_delay=request_delay,
        source_label="search",
    )
    merged = merge_tweets(existing_tweets, search_tweets)

    if community_id:
        refresh_dt = parse_twitter_datetime(refresh_seed or newest_posted_at)
        community_since_timestamp = (
            int(refresh_dt.timestamp())
            if refresh_dt is not None
            else parse_date_to_timestamp(since_date, end_of_day=False)
        )
        until_timestamp = parse_date_to_timestamp(until_date, end_of_day=True) if until_date else None
        print(
            f"Mode: update | community id: {community_id} | refreshing from timestamp {community_since_timestamp}",
            file=sys.stderr,
        )
        community_tweets = fetch_community_tweets(
            session,
            community_id=community_id,
            request_delay=request_delay,
            since_timestamp=community_since_timestamp,
            until_timestamp=until_timestamp,
        )
        merged = merge_tweets(merged, community_tweets)

    print(f"Update complete. Total posts collected: {len(merged)}", file=sys.stderr)
    return merged


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("Missing API key. Pass --api-key or set SOCIALDATA_API_KEY.", file=sys.stderr)
        return 2

    users_path = Path(args.users_file)
    tweets_path = Path(args.tweets_file)

    session = requests.Session()
    session.headers.update(get_headers(args.api_key))

    try:
        existing_tweets = load_existing_tweets(tweets_path)
        community_id = None if args.no_community else args.community_id
        if args.mode == "full":
            tweet_rows = run_full_mode(
                session,
                query=args.query,
                community_id=community_id,
                since_date=args.since_date,
                until_date=args.until_date,
                request_delay=args.request_delay,
            )
        else:
            tweet_rows = run_update_mode(
                session,
                query=args.query,
                community_id=community_id,
                since_date=args.since_date,
                until_date=args.until_date,
                request_delay=args.request_delay,
                existing_tweets=existing_tweets,
                refresh_count=args.refresh_count,
            )
        users_summary = summarize_users(tweet_rows)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        body = exc.response.text if exc.response is not None else str(exc)
        print(f"HTTP error {status_code}: {body}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    write_json(users_path, users_summary)
    write_json(tweets_path, tweet_rows)

    print(f"Users exported: {len(users_summary)} -> {users_path}")
    print(f"Tweets exported: {len(tweet_rows)} -> {tweets_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
