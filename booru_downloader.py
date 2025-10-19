#!/usr/bin/env python3
"""
booru_downloader.py

支持 Danbooru (/posts.json) 与 Gelbooru/Safebooru DAPI
功能：
 - include / exclude tags
 - ratio 或 min-width/min-height 过滤
 - 将每张图片的 tags metadata 同步保存为 txt
 - 简单的速率限制（默认 1 req/sec）
"""
import argparse
import os
import time
import math
import requests
from urllib.parse import urljoin, urlencode
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# ----- Helpers -----
def sane_sleep(interval_s: float):
    if interval_s > 0:
        time.sleep(interval_s)

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def parse_ratio(r: str):
    # accepts "16:9" or "1.777" or "4/3"
    if ":" in r:
        a,b = r.split(":")
        return float(a) / float(b)
    if "/" in r:
        a,b = r.split("/")
        return float(a) / float(b)
    return float(r)

# ----- API builders -----
def is_danbooru_like(base_url: str) -> bool:
    # simple heuristic
    return "danbooru" in base_url or "donmai" in base_url

def build_posts_url(base_url: str, api_type: str, page: int, limit: int, tags: Optional[str]):
    if api_type == "danbooru":
        # https://danbooru.donmai.us/posts.json?tags=...&page=...
        base = base_url.rstrip("/")
        params = {}
        if tags:
            params["tags"] = tags
        params["page"] = page
        params["limit"] = limit
        return f"{base}/posts.json?{urlencode(params)}"
    else:
        # gelbooru / safebooru DAPI:
        # /index.php?page=dapi&s=post&q=index&json=1&tags=...&pid=...&limit=...
        params = {
            "page": "dapi",
            "s": "post",
            "q": "index",
            "json": "1",
            "pid": page,
            "limit": limit,
        }
        if tags:
            params["tags"] = tags
        return f"{base_url.rstrip('/')}/index.php?{urlencode(params)}"

# ----- JSON field access (robust across implementations) -----
def get_image_url_from_post(post: dict, api_type: str) -> Optional[str]:
    # Danbooru: 'file_url' often contains the image link; Gelbooru: 'file_url' or 'image' or 'sample_url'
    for key in ("file_url", "image_url", "image", "large_file_url", "source", "preview_file_url"):
        if key in post and post.get(key):
            return post.get(key)
    # fallbacks for old gelbooru: 'file_url' or 'file_url' might be present
    return None

def get_tags_from_post(post: dict) -> str:
    # Danbooru: 'tag_string' or 'tag_string_general' etc; Gelbooru: 'tags'
    if "tag_string" in post and post["tag_string"] is not None:
        return post["tag_string"]
    if "tags" in post:
        # sometimes space-separated or array
        tags = post["tags"]
        if isinstance(tags, list):
            return " ".join(tags)
        return str(tags)
    return ""

def get_dimensions(post: dict):
    # try several common keys
    w = post.get("image_width") or post.get("width") or post.get("preview_width")
    h = post.get("image_height") or post.get("height") or post.get("preview_height")
    try:
        return int(w), int(h)
    except Exception:
        return None, None

# ----- Downloader -----
def download_file(url: str, dest_path: str, session: requests.Session, headers=None):
    try:
        r = session.get(url, stream=True, headers=headers, timeout=20)
        r.raise_for_status()
    except Exception as e:
        return False, f"download failed: {e}"
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return True, "ok"

# ----- Main worker -----
def fetch_and_download(base_url: str, include_tags: str, exclude_tags: str,
                       ratio: Optional[float], min_w: Optional[int], min_h: Optional[int],
                       out_dir: str, max_images: int, rps: float, per_page: int,
                       api_type: str, username: Optional[str], api_key: Optional[str]):
    ensure_dir(out_dir)
    images_dir = os.path.join(out_dir, "images")
    txt_dir = os.path.join(out_dir, "tags")
    ensure_dir(images_dir)
    ensure_dir(txt_dir)

    session = requests.Session()
    # polite user-agent
    ua = "booru-downloader/1.0 (+https://example.local) Python"
    headers = {"User-Agent": ua}
    # If api_key/username provided for gelbooru (api_key & user_id) or basic auth for danbooru, we can attach them
    auth_params = {}
    if api_key and username and api_type != "danbooru":
        # Gelbooru-style: &api_key=...&user_id=...
        auth_params["api_key"] = api_key
        auth_params["user_id"] = username

    # combine include/exclude tags into a single query string for booru search
    # exclude tag is represented by prefixing '-'
    tag_query_parts = []
    if include_tags:
        tag_query_parts.append(include_tags.strip())
    if exclude_tags:
        # user may supply multiple separated by spaces; prefix each with '-'
        for t in exclude_tags.split():
            t = t.strip()
            if t:
                tag_query_parts.append(f"-{t}")
    tag_query = " ".join(tag_query_parts) if tag_query_parts else None

    downloaded = 0
    page = 0
    # API page semantics:
    # - Danbooru uses page=1,2... (we'll start page=1)
    # - Gelbooru DAPI uses pid=0,1... (pid 0 = first page). We'll map accordingly.
    while downloaded < max_images:
        page += 1
        if api_type == "danbooru":
            req_page = page
        else:
            req_page = page - 1  # pid starts at 0
        url = build_posts_url(base_url, api_type, req_page, per_page, tag_query)
        # for gelbooru add auth params manually
        if auth_params and api_type != "danbooru":
            url = url + "&" + urlencode(auth_params)

        # rate-limit
        sane_sleep(1.0 / rps if rps > 0 else 0)

        try:
            r = session.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[WARN] failed to fetch page {page} ({url}): {e}")
            break

        # data could be dict or list depending on implementation - normalize to list
        posts = data if isinstance(data, list) else data.get("post") or data.get("posts") or data.get("posts") or []
        if isinstance(posts, dict):
            # sometimes the DAPI returns {"post": {...}} for single
            posts = [posts]
        if not posts:
            print("[INFO] no more posts, stopping.")
            break

        for post in posts:
            if downloaded >= max_images:
                break
            # basic metadata
            image_url = get_image_url_from_post(post, api_type)
            if not image_url:
                continue
            tags = get_tags_from_post(post)
            w,h = get_dimensions(post)
            # ratio check
            if ratio and w and h:
                actual_r = float(w) / float(h) if h != 0 else None
                # allow slight tolerance (2%)
                if actual_r is None:
                    continue
                if not (abs(actual_r - ratio) <= ratio * 0.02):  # 2% tolerance
                    continue
            if min_w and (not w or w < min_w):
                continue
            if min_h and (not h or h < min_h):
                continue

            # build filename (use post id if present)
            post_id = post.get("id") or post.get("post_id") or post.get("file_id") or str(int(time.time()*1000))
            # determine extension
            ext = os.path.splitext(image_url.split("?")[0])[1] or ".jpg"
            image_name = f"{post_id}{ext}"
            img_path = os.path.join(images_dir, image_name)
            txt_path = os.path.join(txt_dir, f"{post_id}.txt")

            # skip if exists
            if os.path.exists(img_path):
                print(f"[SKIP] exists: {img_path}")
                downloaded += 1
                continue

            # download image
            ok, msg = download_file(image_url, img_path, session, headers=headers)
            if not ok:
                print(f"[WARN] failed to download {image_url}: {msg}")
                continue

            # save metadata/tags
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"post_id: {post_id}\n")
                f.write(f"image_url: {image_url}\n")
                f.write(f"width: {w}\nheight: {h}\n")
                f.write("tags:\n")
                f.write(tags + "\n")
                # optionally write raw post json
                # f.write("\nRAW_JSON:\n")
                # import json; f.write(json.dumps(post, ensure_ascii=False, indent=2))

            downloaded += 1
            print(f"[DOWN] {downloaded}: {image_url} -> {img_path}")

        # safety: if returned fewer posts than per_page, probably done
        if len(posts) < per_page:
            break

    print(f"[DONE] downloaded {downloaded} images to {images_dir} and tags to {txt_dir}")


# ----- CLI -----
def main():
    p = argparse.ArgumentParser(description="Booru batch downloader (Danbooru/Gelbooru/Safebooru compatible)")
    p.add_argument("--base-url", required=True, help="Base URL, e.g. https://safebooru.org or https://danbooru.donmai.us")
    p.add_argument("--include-tags", default="", help="Space-separated tags to include (e.g. 'touhou remilia')")
    p.add_argument("--exclude-tags", default="", help="Space-separated tags to exclude (e.g. 'rating:explicit')")
    p.add_argument("--ratio", default=None, help="Desired ratio, e.g. '16:9' or '1.777'")
    p.add_argument("--min-width", type=int, default=0)
    p.add_argument("--min-height", type=int, default=0)
    p.add_argument("--output", default="./downloads", help="Output dir")
    p.add_argument("--max-images", type=int, default=100, help="Max images to download")
    p.add_argument("--rps", type=float, default=1.0, help="Requests per second to API (default 1.0)")
    p.add_argument("--per-page", type=int, default=100, help="How many results per API page (respect site limits)")
    p.add_argument("--api-type", choices=("auto","danbooru","dapi"), default="auto", help="API type (auto-detect)")
    p.add_argument("--username", default=None, help="username or user_id for API (if required)")
    p.add_argument("--api-key", default=None, help="api_key for sites that support key-based auth")
    args = p.parse_args()

    api_type = args.api_type
    if api_type == "auto":
        api_type = "danbooru" if is_danbooru_like(args.base_url) else "dapi"

    ratio = None
    if args.ratio:
        ratio = parse_ratio(args.ratio)

    # clamp per_page to safe defaults: Danbooru often has 100 limit, Safebooru docs sometimes allow larger - user can set
    per_page = min(int(args.per_page), 100 if api_type == "danbooru" else 1000)

    fetch_and_download(
        base_url=args.base_url,
        include_tags=args.include_tags,
        exclude_tags=args.exclude_tags,
        ratio=ratio,
        min_w=args.min_width or None,
        min_h=args.min_height or None,
        out_dir=args.output,
        max_images=args.max_images,
        rps=args.rps,
        per_page=per_page,
        api_type=api_type,
        username=args.username,
        api_key=args.api_key
    )

if __name__ == "__main__":
    main()
