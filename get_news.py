import re
import hashlib
from datetime import datetime

import feedparser
import psycopg2
import requests
from bs4 import BeautifulSoup

# Конфигурация
DB_CONFIG = {
    'user': USER,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT,
    'database': DATABASE,
    #'sslmode': 'require'
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
RSS_URL = "https://www.coindesk.com/arc/outboundfeeds/rss/"
REQUEST_TIMEOUT = 10
MIN_TEXT_LENGTH = 100


def create_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    conn = create_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id SERIAL PRIMARY KEY,
                    article_language TEXT NOT NULL,
                    title TEXT NOT NULL,
                    full_text TEXT NOT NULL,
                    author VARCHAR(255),
                    publish_dttm TIMESTAMP,
                    modified_dttm TIMESTAMP,
                    url VARCHAR(512) UNIQUE NOT NULL,
                    tags TEXT[],
                    content_hash CHAR(32) UNIQUE NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cur.execute('''
                CREATE INDEX IF NOT EXISTS idx_content_hash
                ON articles(content_hash)
            ''')
        conn.commit()
    finally:
        conn.close()


def get_article_language(soup):
    html_tag = soup.find("html")
    if html_tag and html_tag.has_attr("lang"):
        return html_tag["lang"]

    meta_language = soup.find("meta", {"name": "content_language"})
    if meta_language:
        return meta_language.get("content", "")

    og_locale = soup.find("meta", {"property": "og:locale"})
    if og_locale:
        return og_locale.get("content", "")

    return "unknown"


def combine_date_time(date_meta, time_meta):
    if not date_meta:
        return None

    try:
        article_date = datetime.strptime(date_meta.get("content", ""), "%Y%m%d").date()
    except ValueError:
        return None

    article_time = datetime.min.time()
    if time_meta:
        try:
            article_time = datetime.strptime(time_meta.get("content", ""), "%H:%M").time()
        except ValueError:
            pass

    return datetime.combine(article_date, article_time)


def extract_metadata(soup):
    metadata = {
        "author": "",
        "publish_dttm": None,
        "modified_dttm": None,
        "tags": [],
    }

    author_meta = soup.find("meta", {"name": "authors"})
    if author_meta:
        metadata["author"] = author_meta.get("content", "")

    tags_meta = soup.find("meta", {"name": "tags"})
    if tags_meta:
        metadata["tags"] = [tag.strip() for tag in tags_meta.get("content", "").split(",") if tag.strip()]

    metadata["publish_dttm"] = combine_date_time(
        soup.find("meta", {"name": "publish_date"}),
        soup.find("meta", {"name": "publish_time"})
    )

    metadata["modified_dttm"] = combine_date_time(
        soup.find("meta", {"name": "last_modified_date"}),
        soup.find("meta", {"name": "last_modified_time"})
    )

    if metadata["publish_dttm"] and not metadata["modified_dttm"]:
        metadata["modified_dttm"] = metadata["publish_dttm"]
    if metadata["modified_dttm"] and not metadata["publish_dttm"]:
        metadata["publish_dttm"] = metadata["modified_dttm"]

    return metadata


def clean_html(raw_html):
    clean = re.compile(r"<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});")
    return re.sub(clean, "", raw_html or "")


def extract_text_from_soup(soup):
    selectors = [
        ("section", {"class": "flex-grow container"}),
        ("article", {}),
        ("main", {}),
    ]

    for tag_name, attrs in selectors:
        container = soup.find(tag_name, attrs)
        if not container:
            continue

        paragraphs = []
        for p in container.find_all("p"):
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            if text.strip() == "More For You":
                break
            paragraphs.append(text)

        full_text = "\n".join(paragraphs).strip()
        if full_text:
            return full_text

    paragraphs = [
        p.get_text(" ", strip=True)
        for p in soup.find_all("p")
        if p.get_text(strip=True)
    ]
    return "\n".join(paragraphs).strip()


def get_feed_text(entry):
    if hasattr(entry, "content"):
        for item in entry.content:
            value = clean_html(item.get("value", ""))
            if value.strip():
                return value.strip()

    summary = clean_html(getattr(entry, "summary", ""))
    return summary.strip()


def calculate_hash(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def parse_article(url):
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    metadata = extract_metadata(soup)
    full_text = extract_text_from_soup(soup)

    return {
        "article_language": get_article_language(soup),
        "author": metadata["author"],
        "publish_dttm": metadata["publish_dttm"],
        "modified_dttm": metadata["modified_dttm"],
        "tags": metadata["tags"],
        "full_text": clean_html(full_text),
    }


def save_article(conn, entry, article_data):
    title = clean_html(getattr(entry, "title", ""))
    full_text = clean_html(article_data["full_text"])
    content_hash = calculate_hash(full_text)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO articles (
                article_language, title, full_text, author,
                publish_dttm, modified_dttm, url, tags, content_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                article_language = EXCLUDED.article_language,
                title = EXCLUDED.title,
                full_text = EXCLUDED.full_text,
                author = EXCLUDED.author,
                publish_dttm = EXCLUDED.publish_dttm,
                modified_dttm = EXCLUDED.modified_dttm,
                tags = EXCLUDED.tags,
                content_hash = EXCLUDED.content_hash,
                last_updated = CURRENT_TIMESTAMP
        """, (
            article_data["article_language"],
            title,
            full_text,
            article_data["author"] or None,
            article_data["publish_dttm"],
            article_data["modified_dttm"],
            entry.link,
            article_data["tags"] if article_data["tags"] else None,
            content_hash,
        ))


init_db()
conn = create_db_connection()

saved = 0
skipped = 0

try:
    feed = feedparser.parse(RSS_URL)

    for entry in feed.entries:
        try:
            article_data = parse_article(entry.link)

            if len(article_data["full_text"]) < MIN_TEXT_LENGTH:
                fallback_text = get_feed_text(entry)
                if fallback_text:
                    article_data["full_text"] = fallback_text

            if not article_data["full_text"].strip():
                skipped += 1
                continue

            save_article(conn, entry, article_data)
            conn.commit()
            saved += 1
            print(f"Saved: {entry.title}")

        except Exception as e:
            conn.rollback()
            print(f"Error processing {entry.link}: {e}")
            skipped += 1

finally:
    conn.close()

print(f"Done. Saved: {saved}, Skipped: {skipped}")
