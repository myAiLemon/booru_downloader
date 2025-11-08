#!/usr/bin/env python3

import argparse
import os
import time
import math
import requests
from urllib.parse import urljoin, urlencode
from pathlib import Path
from typing import Optional, List
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

def get_score(post: dict) -> Optional[int]:
    for k in ("score", "total_score", "rating_score", "fav_count", "up_score"):
        if k in post:
            try:
                return int(post[k])
            except Exception:
                continue
    return None

# ----- Downloader -----
def download_file(url: str, dest_path: str, session: requests.Session, headers=None):
    try:
        # <--- 修改：不再传入 headers，因为 session 已经包含了
        # session.get 会自动使用 session 级别的 headers 和 proxies
        r = session.get(url, stream=True, timeout=20) 
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
                       ratios: Optional[List[float]], min_w: Optional[int], min_h: Optional[int],
                       min_score: Optional[int], 
                       out_dir: str, max_images: int, rps: float, per_page: int,
                       api_type: str, username: Optional[str], api_key: Optional[str],
                       proxy: Optional[str]):
    ensure_dir(out_dir)
    images_dir = os.path.join(out_dir, "images")
    txt_dir = os.path.join(out_dir, "tags")
    ensure_dir(images_dir)
    ensure_dir(txt_dir)

    # 过滤扩展名（修复：确保 .gif 带点号）
    VIDEO_EXTENSIONS = {'.mp4', '.webm', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.gifv', '.gif'}

    session = requests.Session()
    # polite user-agent
    ua = "booru-downloader/1.0 (+https://example.local) Python"
    
    session.headers.update({"User-Agent": ua})
    
    # 设置代理
    if proxy:
        print(f"[INFO] Using proxy: {proxy}")
        proxies_dict = {
            'http': proxy,
            'https': proxy
        }
        session.proxies.update(proxies_dict)

    auth = None
    auth_params = {}
    
    if api_type == "danbooru" and username and api_key:
        auth = (username, api_key)
    elif api_key and username and api_type != "danbooru":
        auth_params["api_key"] = api_key
        auth_params["user_id"] = username

    tag_query_parts = []

    if api_type != "danbooru" and api_key and username:
        pass

    if include_tags:
        tag_query_parts.append(include_tags.strip())
    if exclude_tags:
        for t in exclude_tags.split():
            t = t.strip()
            if t:
                tag_query_parts.append(f"-{t}")
    tag_query = " ".join(tag_query_parts) if tag_query_parts else None

    downloaded = 0
    page = 0
    
    while downloaded < max_images:
        page += 1
        if api_type == "danbooru":
            req_page = page
        else:
            req_page = page - 1
        url = build_posts_url(base_url, api_type, req_page, per_page, tag_query)
        
        if auth_params and api_type != "danbooru":
            url = url + "&" + urlencode(auth_params)

        # rate-limit
        sane_sleep(1.0 / rps if rps > 0 else 0)

        try:
            r = session.get(url, auth=auth, timeout=30) 
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[WARN] failed to fetch page {page} ({url}): {e}")
            break

        posts = data if isinstance(data, list) else data.get("post") or data.get("posts") or data.get("posts") or []
        if isinstance(posts, dict):
            posts = [posts]
        if not posts:
            print("[INFO] no more posts, stopping.")
            break

        for post in posts:
            if downloaded >= max_images:
                break
            
            # 1. 尝试获取图片URL
            image_url = get_image_url_from_post(post, api_type)
            if not image_url:
                # 无URL则跳过
                continue
            
            # 2. 解析URL中的扩展名
            try:
                # 分割URL参数部分，提取基础URL后获取扩展名
                ext = os.path.splitext(image_url.split("?")[0])[1].lower()
            except Exception:
                # 解析失败则跳过
                continue

            # 3. 检查是否为视频/动画格式
            if ext in VIDEO_EXTENSIONS:
                print(f"[SKIP] Video/Animation: {image_url}")
                continue

            tags = get_tags_from_post(post)
            w,h = get_dimensions(post)
            
            # --- 过滤逻辑 ---
            if ratios and w and h:
                actual_r = float(w) / float(h) if h != 0 else None
                if actual_r is None:
                    continue
                matches_any_ratio = False
                for target_ratio in ratios:
                    if (abs(actual_r - target_ratio) <= target_ratio * 0.02):
                        matches_any_ratio = True
                        break 
                if not matches_any_ratio:
                    continue
            if min_w and (not w or w < min_w):
                continue
            if min_h and (not h or h < min_h):
                continue
            if min_score is not None:
                score = get_score(post)
                if score is None or score < min_score:
                    continue
            # --- 过滤逻辑结束 ---

            # 处理文件名
            post_id = post.get("id") or post.get("post_id") or post.get("file_id") or str(int(time.time()*1000))
            
            # 确保扩展名存在
            if not ext:
                ext = ".jpg"
                
            image_name = f"{post_id}{ext}"
            img_path = os.path.join(images_dir, image_name)
            txt_path = os.path.join(txt_dir, f"{post_id}.txt")

            if os.path.exists(img_path):
                print(f"[SKIP] exists: {img_path}")
                downloaded += 1
                continue

            ok, msg = download_file(image_url, img_path, session)
            if not ok:
                print(f"[WARN] failed to download {image_url}: {msg}")
                continue

            # 保存标签
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(tags)

            downloaded += 1
            print(f"[DOWN] {downloaded}: {image_url} -> {img_path}")

        if len(posts) < per_page:
            break

    print(f"[DONE] downloaded {downloaded} images to {images_dir} and tags to {txt_dir}")


# ----- CLI -----
def main():
    p = argparse.ArgumentParser(description="Booru batch downloader (Danbooru/Gelbooru/Safebooru compatible)")
    p.add_argument("--base-url", required=True, help="Base URL, e.g. https://safebooru.org or https://danbooru.donmai.us")
    p.add_argument("--include-tags", default="", help="Space-separated tags to include (e.g. 'touhou remilia')")
    p.add_argument("--exclude-tags", default="", help="Space-separated tags to exclude (e.g. 'rating:explicit')")
    p.add_argument("--ratio", default=None, nargs='+', help="Desired ratio(s), e.g. '16:9' or '1.777' or '16:9 1:1 4:3'")
    p.add_argument("--min-width", type=int, default=0)
    p.add_argument("--min-height", type=int, default=0)
    p.add_argument("--min-score", type=int, default=None, help="Minimum score to download (e.g. 50)")
    p.add_argument("--output", default="./downloads", help="Output dir")
    p.add_argument("--max-images", type=int, default=100, help="Max images to download")
    p.add_argument("--rps", type=float, default=1.0, help="Requests per second to API (default 1.0)")
    p.add_argument("--per-page", type=int, default=100, help="How many results per API page (respect site limits)")
    p.add_argument("--api-type", choices=("auto","danbooru","dapi"), default="auto", help="API type (auto-detect)")
    p.add_argument("--username", default=None, help="username or user_id for API (if required)")
    p.add_argument("--api-key", default=None, help="api_key for sites that support key-based auth")
    p.add_argument("--proxy", default=None, help="Proxy URL (e.g., http://127.0.0.1:7890 or socks5://127.0.0.1:1080)")
    
    args = p.parse_args()

    api_type = args.api_type
    if api_type == "auto":
        api_type = "danbooru" if is_danbooru_like(args.base_url) else "dapi"

    ratios = None
    if args.ratio:
        try:
            ratios = [parse_ratio(r) for r in args.ratio]
        except ValueError:
            print(f"Error: Invalid ratio format in {args.ratio}. Use '16:9' or '1.777'.")
            return

    per_page = min(int(args.per_page), 100 if api_type == "danbooru" else 1000)

    fetch_and_download(
        base_url=args.base_url,
        include_tags=args.include_tags,
        exclude_tags=args.exclude_tags,
        ratios=ratios,
        min_w=args.min_width or None,
        min_h=args.min_height or None,
        min_score=args.min_score,
        out_dir=args.output,
        max_images=args.max_images,
        rps=args.rps,
        per_page=per_page,
        api_type=api_type,
        username=args.username,
        api_key=args.api_key,
        proxy=args.proxy
    )

if __name__ == "__main__":
    main()
