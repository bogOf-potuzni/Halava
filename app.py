import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import discord
import feedparser
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))
CODE_PATTERN = re.compile(r"\b[A-Z0-9]{4,20}(?:-[A-Z0-9]{2,12}){0,3}\b")


@dataclass(slots=True)
class Candidate:
    source_name: str
    source_type: str
    company: str
    category: str
    title: str
    summary: str
    published_at: datetime
    author: str
    external_url: str
    codes: list[str]
    verdict: str
    verdict_rank: int
    confidence_label: str
    estimated_lifetime: str
    activation_risk: str
    availability_type: str
    estimated_effect: str
    popularity_label: str
    repeat_minutes: int | None
    identity_key: str


class Storage:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_candidates (
                identity_key TEXT PRIMARY KEY,
                verdict_rank INTEGER NOT NULL,
                sent_at TEXT NOT NULL,
                published_at TEXT NOT NULL,
                code TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def should_send(self, candidate: Candidate, now: datetime, bad_repeat_minutes: int) -> bool:
        row = self.connection.execute(
            "SELECT verdict_rank, sent_at FROM sent_candidates WHERE identity_key = ?",
            (candidate.identity_key,),
        ).fetchone()
        if row is None:
            return True

        if candidate.verdict_rank < 5:
            return False

        sent_at = datetime.fromisoformat(row["sent_at"])
        return now - sent_at >= timedelta(minutes=bad_repeat_minutes)

    def mark_sent(self, candidate: Candidate, now: datetime) -> None:
        self.connection.execute(
            """
            INSERT INTO sent_candidates (identity_key, verdict_rank, sent_at, published_at, code)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(identity_key) DO UPDATE SET
                verdict_rank = excluded.verdict_rank,
                sent_at = excluded.sent_at,
                published_at = excluded.published_at,
                code = excluded.code
            """,
            (
                candidate.identity_key,
                candidate.verdict_rank,
                now.isoformat(),
                candidate.published_at.isoformat(),
                ",".join(candidate.codes),
            ),
        )
        self.connection.commit()


def load_config(config_path: Path) -> dict[str, Any]:
    return json.loads(config_path.read_text(encoding="utf-8"))


def normalize_text(value: str) -> str:
    return " ".join(unescape(value).split())


def html_to_text(value: str) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "lxml")
    return normalize_text(soup.get_text(" ", strip=True))


def parse_datetime(entry: Any) -> datetime | None:
    for key in ("published", "updated", "created"):
        raw_value = entry.get(key)
        if not raw_value:
            continue
        try:
            parsed = parsedate_to_datetime(raw_value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except (TypeError, ValueError):
            continue
    return None


def compile_query(company: str, terms: list[str]) -> str:
    parts = [quote_plus(f'"{company}" "{term}"') for term in terms]
    return " OR ".join(parts)


def extract_codes(text: str) -> list[str]:
    ignore = {"HTTP", "HTTPS", "REDDIT", "DISCORD", "OPENAI", "CHATGPT", "PROMO", "CREDITS"}
    seen: set[str] = set()
    codes: list[str] = []
    for match in CODE_PATTERN.findall(text.upper()):
        if match in ignore or match.isdigit():
            continue
        if sum(character.isdigit() for character in match) == 0 and len(match) < 6:
            continue
        if match not in seen:
            seen.add(match)
            codes.append(match)
    return codes


def estimate_effect(text: str) -> str:
    patterns = [
        (r"(\d+\s*(?:months?|days?|weeks?))\s+(?:free|trial|plus|pro)", None),
        (r"(\d+\s*(?:credits?|tokens?))", None),
        (r"(\d+%\s*(?:off|discount))", None),
        (r"(free\s+trial)", None),
        (r"(student\s+offer)", None),
    ]
    lowered = text.lower()
    for pattern, _ in patterns:
        match = re.search(pattern, lowered)
        if match:
            return match.group(1)
    return "неясно"


def hours_to_text(min_hours: int, max_hours: int | None) -> str:
    if max_hours is None:
        return f"{min_hours}+ ч"
    return f"{min_hours}-{max_hours} ч"


def assess_candidate(
    text: str,
    source_type: str,
    company: str,
    published_at: datetime,
    now: datetime,
    config: dict[str, Any],
) -> tuple[int, str, str, str, str, str]:
    lowered = text.lower()
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600)
    score = 0

    positive_hits = sum(1 for word in config["keyword_groups"]["positive"] if word in lowered)
    negative_hits = sum(1 for word in config["keyword_groups"]["negative"] if word in lowered)
    score += positive_hits * 2
    score -= negative_hits * 3

    aliases = config["company_aliases"].get(company, [])
    if any(alias in lowered for alias in aliases):
        score += 2

    codes = extract_codes(text)
    if codes:
        score += 4

    if source_type == "reddit":
        score += 1
    elif source_type == "news":
        score += 2

    if age_hours <= 3:
        score += 4
    elif age_hours <= 12:
        score += 3
    elif age_hours <= 24:
        score += 2
    elif age_hours <= 72:
        score += 1
    else:
        score -= 5

    if "official" in lowered:
        score += 3
    if "referral" in lowered:
        score -= 1

    if score >= 12:
        return 1, "лучший", "высокая", hours_to_text(12, 72), "низкий", "массовый"
    if score >= 9:
        return 2, "хороший", "выше средней", hours_to_text(6, 24), "средний", "ограниченный"
    if score >= 6:
        return 3, "средний", "средняя", hours_to_text(3, 12), "средний", "ограниченный"
    if score >= 3:
        return 4, "ниже среднего", "ниже средней", hours_to_text(1, 6), "высокий", "неясно"
    return 5, "плохой", "низкая", hours_to_text(0, 3), "очень высокий", "неясно"


def popularity_label(text: str) -> str:
    lowered = text.lower()
    patterns = {
        "высокая": ["viral", "trending", "many users", "hot", "popular"],
        "средняя": ["comments", "upvotes", "likes", "shared"],
    }
    for label, terms in patterns.items():
        if any(term in lowered for term in terms):
            return label
    return "неясно"


def source_age_allowed(rank: int, published_at: datetime, now: datetime, config: dict[str, Any]) -> bool:
    age_hours = (now - published_at).total_seconds() / 3600
    if rank == 5:
        return age_hours <= config["max_bad_post_age_hours"]
    return age_hours <= config["max_post_age_hours"]


def build_identity_key(company: str, codes: list[str], title: str, published_at: datetime) -> str:
    base = f"{company}|{'/'.join(codes)}|{title.lower()}|{published_at.isoformat()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


async def fetch_feed(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url, timeout=20.0, follow_redirects=True)
    response.raise_for_status()
    return feedparser.parse(response.text)


async def collect_candidates(client: httpx.AsyncClient, config: dict[str, Any]) -> list[Candidate]:
    now = datetime.now(UTC)
    candidates: list[Candidate] = []

    for source in config["sources"]:
        try:
            parsed = await fetch_feed(client, source["url"])
        except Exception as error:  # noqa: BLE001
            logging.warning("Source fetch failed for %s: %s", source["name"], error)
            continue

        for entry in parsed.entries:
            published_at = parse_datetime(entry)
            if published_at is None:
                continue

            title = normalize_text(entry.get("title", ""))
            summary = html_to_text(entry.get("summary", ""))
            text = f"{title} {summary}"
            verdict_rank, verdict, confidence, lifetime, risk, availability = assess_candidate(
                text=text,
                source_type=source["source_type"],
                company=source["company"],
                published_at=published_at,
                now=now,
                config=config,
            )
            if not source_age_allowed(verdict_rank, published_at, now, config):
                continue

            codes = extract_codes(text)
            if not codes:
                continue

            candidate = Candidate(
                source_name=source["name"],
                source_type=source["source_type"],
                company=source["company"],
                category=source["category"],
                title=title,
                summary=summary,
                published_at=published_at,
                author=normalize_text(entry.get("author", "")) or "неизвестно",
                external_url=entry.get("link", ""),
                codes=codes[:3],
                verdict=verdict,
                verdict_rank=verdict_rank,
                confidence_label=confidence,
                estimated_lifetime=lifetime,
                activation_risk=risk,
                availability_type=availability,
                estimated_effect=estimate_effect(text),
                popularity_label=popularity_label(text),
                repeat_minutes=config["bad_repeat_minutes"] if verdict_rank == 5 else None,
                identity_key=build_identity_key(source["company"], codes[:3], title, published_at),
            )
            candidates.append(candidate)

    candidates.sort(key=lambda item: (item.verdict_rank, item.published_at), reverse=False)
    return candidates


def verdict_emoji(rank: int) -> str:
    return {
        1: "🟢",
        2: "🔵",
        3: "🟡",
        4: "🟠",
        5: "🔴",
    }[rank]


def format_datetime(value: datetime) -> str:
    return value.astimezone(MSK).strftime("%d.%m.%Y %H:%M MSK")


def candidate_to_embed(candidate: Candidate) -> discord.Embed:
    embed = discord.Embed(
        title=f"{verdict_emoji(candidate.verdict_rank)} {candidate.company} | {candidate.verdict.title()}",
        description=candidate.title[:4000] if candidate.title else "Найдено новое упоминание промокода",
        color={
            1: 0x2ECC71,
            2: 0x3498DB,
            3: 0xF1C40F,
            4: 0xE67E22,
            5: 0xE74C3C,
        }[candidate.verdict_rank],
    )
    embed.add_field(name="Промокод", value="\n".join(f"`{code}`" for code in candidate.codes), inline=False)
    embed.add_field(name="Категория", value=candidate.category, inline=True)
    embed.add_field(name="Источник", value=f"{candidate.source_type} / {candidate.source_name}", inline=True)
    embed.add_field(name="Дата поста", value=format_datetime(candidate.published_at), inline=True)
    embed.add_field(name="Примерное действие", value=candidate.estimated_effect, inline=True)
    embed.add_field(name="Вероятность, что рабочий", value=candidate.confidence_label, inline=True)
    embed.add_field(name="Срок жизни", value=candidate.estimated_lifetime, inline=True)
    embed.add_field(name="Риск, что уже активирован", value=candidate.activation_risk, inline=True)
    embed.add_field(name="Тип доступности", value=candidate.availability_type, inline=True)
    embed.add_field(name="Популярность сигнала", value=candidate.popularity_label, inline=True)
    if candidate.summary:
        embed.add_field(name="Контекст", value=candidate.summary[:1024], inline=False)
    embed.set_footer(text=f"Автор: {candidate.author}")
    return embed


class PromoWatcherClient(discord.Client):
    def __init__(self, config: dict[str, Any], storage: Storage) -> None:
        intents = discord.Intents.none()
        super().__init__(intents=intents)
        self.config = config
        self.storage = storage
        self.http_client = httpx.AsyncClient(
            headers={
                "User-Agent": "promo-watcher-bot/1.0 (+discord alert bot)",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self.poll_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self.poll_task = asyncio.create_task(self.poll_loop())

    async def close(self) -> None:
        if self.poll_task:
            self.poll_task.cancel()
        await self.http_client.aclose()
        await super().close()

    async def on_ready(self) -> None:
        logging.info("Discord bot connected as %s", self.user)

    async def poll_loop(self) -> None:
        await self.wait_until_ready()
        channel_id = int(os.environ["DISCORD_CHANNEL_ID"])
        channel = self.get_channel(channel_id)
        if channel is None:
            channel = await self.fetch_channel(channel_id)

        while not self.is_closed():
            now = datetime.now(UTC)
            try:
                candidates = await collect_candidates(self.http_client, self.config)
                for candidate in candidates:
                    if not self.storage.should_send(candidate, now, self.config["bad_repeat_minutes"]):
                        continue
                    await channel.send(embed=candidate_to_embed(candidate))
                    self.storage.mark_sent(candidate, now)
                    await asyncio.sleep(1.0)
                logging.info("Polling cycle completed: %s candidates checked", len(candidates))
            except Exception as error:  # noqa: BLE001
                logging.exception("Polling cycle failed: %s", error)

            await asyncio.sleep(self.config["poll_interval_minutes"] * 60)


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    config_path = Path(os.environ.get("APP_CONFIG_PATH", "config.json"))
    config = load_config(config_path)
    storage = Storage(Path("data") / "sent_candidates.sqlite3")

    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")

    client = PromoWatcherClient(config=config, storage=storage)
    client.run(token, log_handler=None)


if __name__ == "__main__":
    main()
