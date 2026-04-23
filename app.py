import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import discord
import feedparser
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))

CODE_PATTERN = re.compile(r"\b[A-Z0-9]{4,24}(?:-[A-Z0-9]{2,12}){0,3}\b")
COMMON_UPPERCASE_WORDS = {
    "ABOUT",
    "ACCESS",
    "ACCOUNT",
    "ACTIVE",
    "ADWEEK",
    "AI",
    "APRIL",
    "AUDIO",
    "AVAILABLE",
    "BRANDS",
    "BUSINESS",
    "CHATGPT",
    "CLAUDE",
    "CODE",
    "CODES",
    "CREDIT",
    "CREDITS",
    "DAY",
    "DAYS",
    "DEAL",
    "DISCOUNT",
    "FREE",
    "FROM",
    "GOOD",
    "GPT",
    "GUIDE",
    "HOURS",
    "MONTH",
    "MONTHS",
    "NEWS",
    "NOW",
    "OFFER",
    "OPENAI",
    "PLAN",
    "PLUS",
    "POST",
    "PROMO",
    "PROMOCODE",
    "RUNWAY",
    "SALE",
    "SAVE",
    "STUDENT",
    "SUBSCRIPTION",
    "THIS",
    "TODAY",
    "TRIAL",
    "UNIVERSE",
    "VIDEO",
    "WITH",
    "YEAR",
    "YEARS",
}
OFFER_PATTERNS: list[tuple[str, str]] = [
    ("free trial", r"\bfree\s+trial\b"),
    ("student offer", r"\bstudent\s+offer\b"),
    ("student discount", r"\bstudent\s+discount\b"),
    ("credits", r"\b\d+\s*(?:credits?|tokens?)\b"),
    ("free month", r"\b\d*\s*(?:free\s+months?|months?\s+free|free\s+month)\b"),
    ("free days", r"\b\d+\s*(?:days?|weeks?)\s+free\b"),
    ("discount", r"\b\d+%\s*(?:off|discount)\b"),
    ("bonus", r"\bbonus\b"),
    ("gift", r"\bgift\b"),
    ("redeem", r"\bredeem\b"),
    ("voucher", r"\bvoucher\b"),
]
NUMERIC_CONFIG_KEYS = (
    "poll_interval_minutes",
    "max_best_post_age_hours",
    "max_good_post_age_hours",
    "max_medium_post_age_hours",
    "max_low_post_age_hours",
    "max_bad_post_age_hours",
    "bad_repeat_minutes",
    "max_messages_per_cycle",
)
EMBED_COLORS = {
    1: 0x2ECC71,
    2: 0x3498DB,
    3: 0xF1C40F,
    4: 0xE67E22,
    5: 0xE74C3C,
}


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
    offers: list[str]
    verdict: str
    verdict_rank: int
    confidence_label: str
    estimated_lifetime: str
    activation_risk: str
    availability_type: str
    estimated_effect: str
    popularity_label: str
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

        previous_rank = int(row["verdict_rank"])
        if candidate.verdict_rank < previous_rank:
            return True
        if candidate.verdict_rank < 5:
            return False

        sent_at = datetime.fromisoformat(row["sent_at"])
        return now - sent_at >= timedelta(minutes=bad_repeat_minutes)

    def mark_sent(self, candidate: Candidate, now: datetime) -> None:
        preview = candidate.codes or candidate.offers
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
                ",".join(preview),
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def load_config(config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    config.setdefault("max_messages_per_cycle", 10)
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    required_root_keys = {
        "poll_interval_minutes",
        "max_best_post_age_hours",
        "max_good_post_age_hours",
        "max_medium_post_age_hours",
        "max_low_post_age_hours",
        "max_bad_post_age_hours",
        "bad_repeat_minutes",
        "sources",
        "keyword_groups",
        "company_aliases",
    }
    missing_root_keys = sorted(required_root_keys - set(config))
    if missing_root_keys:
        raise RuntimeError(f"Missing config keys: {', '.join(missing_root_keys)}")

    for key in NUMERIC_CONFIG_KEYS:
        value = config.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise RuntimeError(f"{key} must be a positive integer")

    keyword_groups = config["keyword_groups"]
    if not isinstance(keyword_groups, dict):
        raise RuntimeError("keyword_groups must be an object")
    for group_name in ("positive", "negative"):
        group_values = keyword_groups.get(group_name)
        if not isinstance(group_values, list) or not group_values:
            raise RuntimeError(f"keyword_groups.{group_name} must be a non-empty list")
        if any(not isinstance(item, str) or not item.strip() for item in group_values):
            raise RuntimeError(f"keyword_groups.{group_name} must contain non-empty strings")

    company_aliases = config["company_aliases"]
    if not isinstance(company_aliases, dict) or not company_aliases:
        raise RuntimeError("company_aliases must be a non-empty object")
    for company, aliases in company_aliases.items():
        if not isinstance(company, str) or not company.strip():
            raise RuntimeError("company_aliases keys must be non-empty strings")
        if not isinstance(aliases, list):
            raise RuntimeError(f"company_aliases.{company} must be a list")
        if any(not isinstance(alias, str) or not alias.strip() for alias in aliases):
            raise RuntimeError(f"company_aliases.{company} must contain non-empty strings")

    sources = config["sources"]
    if not isinstance(sources, list) or not sources:
        raise RuntimeError("sources must be a non-empty list")

    required_source_keys = {"name", "kind", "company", "category", "url", "source_type"}
    for index, source in enumerate(sources, start=1):
        if not isinstance(source, dict):
            raise RuntimeError(f"source #{index} must be an object")

        missing_source_keys = sorted(required_source_keys - set(source))
        if missing_source_keys:
            raise RuntimeError(f"source #{index} is missing keys: {', '.join(missing_source_keys)}")

        for key in required_source_keys:
            value = source[key]
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"source #{index}.{key} must be a non-empty string")

        if source["kind"] != "rss":
            raise RuntimeError(f"source #{index}.kind must be 'rss'")
        if not source["url"].startswith(("http://", "https://")):
            raise RuntimeError(f"source #{index}.url must start with http:// or https://")


def normalize_text(value: str) -> str:
    return " ".join(unescape(value or "").split())


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
        except (TypeError, ValueError):
            continue

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def extract_codes(text: str) -> list[str]:
    ignore = {"HTTP", "HTTPS", "REDDIT", "DISCORD", "OPENAI", "CHATGPT", "PROMO", "CREDITS"}
    seen: set[str] = set()
    codes: list[str] = []

    for match in CODE_PATTERN.findall(text.upper()):
        digit_count = sum(character.isdigit() for character in match)
        if match in ignore or match.isdigit() or match in COMMON_UPPERCASE_WORDS:
            continue
        if digit_count < 2:
            continue
        if len(match.replace("-", "")) < 6:
            continue
        if match in seen:
            continue
        seen.add(match)
        codes.append(match)

    return codes


def extract_offer_signals(text: str) -> list[str]:
    lowered = text.lower()
    found: list[str] = []
    seen: set[str] = set()

    for label, pattern in OFFER_PATTERNS:
        if re.search(pattern, lowered) and label not in seen:
            seen.add(label)
            found.append(label)

    for keyword in ("trial", "credits", "credit", "student", "discount", "offer", "coupon"):
        if keyword in lowered and keyword not in seen:
            seen.add(keyword)
            found.append(keyword)

    return found


def estimate_effect(text: str) -> str:
    lowered = text.lower()
    patterns = [
        r"(\d+\s*(?:months?|days?|weeks?))\s+(?:free|trial|plus|pro)",
        r"(\d+\s*(?:credits?|tokens?))",
        r"(\d+%\s*(?:off|discount))",
        r"(free\s+trial)",
        r"(student\s+offer)",
    ]
    for pattern in patterns:
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
    has_codes: bool,
    has_offers: bool,
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

    if has_codes:
        score += 4
    if has_offers:
        score += 3

    if source_type in {"reddit", "news"}:
        score += 1

    if age_hours <= 1:
        score += 5
    elif age_hours <= 3:
        score += 4
    elif age_hours <= 6:
        score += 2
    elif age_hours <= 12:
        score += 1
    else:
        score -= 7

    if "official" in lowered:
        score += 3
    if "referral" in lowered:
        score -= 1

    if score >= 13:
        return 1, "лучший", "высокая", hours_to_text(1, 3), "низкий", "массовый"
    if score >= 10:
        return 2, "хороший", "выше средней", hours_to_text(1, 3), "средний", "ограниченный"
    if score >= 7:
        return 3, "средний", "средняя", hours_to_text(1, 6), "средний", "ограниченный"
    if score >= 4:
        return 4, "ниже среднего", "ниже средней", hours_to_text(1, 4), "высокий", "неясно"
    return 5, "плохой", "низкая", hours_to_text(0, 2), "очень высокий", "неясно"


def popularity_label(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("viral", "trending", "many users", "hot", "popular")):
        return "высокая"
    if any(term in lowered for term in ("comments", "upvotes", "likes", "shared")):
        return "средняя"
    return "неясно"


def source_age_allowed(rank: int, published_at: datetime, now: datetime, config: dict[str, Any]) -> bool:
    age_hours = (now - published_at).total_seconds() / 3600
    if rank == 1:
        return age_hours <= config["max_best_post_age_hours"]
    if rank == 2:
        return age_hours <= config["max_good_post_age_hours"]
    if rank == 3:
        return age_hours <= config["max_medium_post_age_hours"]
    if rank == 4:
        return age_hours <= config["max_low_post_age_hours"]
    if rank == 5:
        return age_hours <= config["max_bad_post_age_hours"]
    return False


def build_identity_key(company: str, codes: list[str], offers: list[str], fallback_text: str) -> str:
    normalized_codes = "/".join(sorted(code.upper() for code in codes))
    normalized_offers = "/".join(sorted(offer.lower() for offer in offers))
    if normalized_codes or normalized_offers:
        base = f"{company.lower()}|{normalized_codes}|{normalized_offers}"
    else:
        normalized_fallback = normalize_text(fallback_text).lower()[:160]
        base = f"{company.lower()}|{normalized_fallback}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


async def fetch_feed(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url, timeout=20.0, follow_redirects=True)
    response.raise_for_status()
    return feedparser.parse(response.text)


def should_replace_candidate(current: Candidate | None, new: Candidate) -> bool:
    if current is None:
        return True
    if new.verdict_rank < current.verdict_rank:
        return True
    if new.verdict_rank == current.verdict_rank and new.published_at > current.published_at:
        return True
    return False


async def collect_candidates(client: httpx.AsyncClient, config: dict[str, Any]) -> list[Candidate]:
    now = datetime.now(UTC)
    parsed_results = await asyncio.gather(
        *(fetch_feed(client, source["url"]) for source in config["sources"]),
        return_exceptions=True,
    )

    unique_candidates: dict[str, Candidate] = {}
    for source, parsed in zip(config["sources"], parsed_results):
        if isinstance(parsed, Exception):
            logging.warning("Source fetch failed for %s: %s", source["name"], parsed)
            continue

        for entry in getattr(parsed, "entries", []):
            published_at = parse_datetime(entry)
            if published_at is None:
                continue

            title = normalize_text(entry.get("title", ""))
            summary = html_to_text(entry.get("summary", ""))
            text = f"{title} {summary}".strip()
            codes = extract_codes(text)
            offers = extract_offer_signals(text)
            if not codes and not offers:
                continue

            verdict_rank, verdict, confidence, lifetime, risk, availability = assess_candidate(
                text=text,
                source_type=source["source_type"],
                company=source["company"],
                published_at=published_at,
                now=now,
                config=config,
                has_codes=bool(codes),
                has_offers=bool(offers),
            )
            if not source_age_allowed(verdict_rank, published_at, now, config):
                continue

            identity_key = build_identity_key(source["company"], codes[:3], offers[:5], title or summary)
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
                offers=offers[:5],
                verdict=verdict,
                verdict_rank=verdict_rank,
                confidence_label=confidence,
                estimated_lifetime=lifetime,
                activation_risk=risk,
                availability_type=availability,
                estimated_effect=estimate_effect(text),
                popularity_label=popularity_label(text),
                identity_key=identity_key,
            )

            current = unique_candidates.get(identity_key)
            if should_replace_candidate(current, candidate):
                unique_candidates[identity_key] = candidate

    return sorted(
        unique_candidates.values(),
        key=lambda item: (item.verdict_rank, -item.published_at.timestamp()),
    )


def verdict_emoji(rank: int) -> str:
    return {1: "🟢", 2: "🔵", 3: "🟡", 4: "🟠", 5: "🔴"}[rank]


def format_datetime(value: datetime) -> str:
    return value.astimezone(MSK).strftime("%d.%m.%Y %H:%M MSK")


def candidate_to_embed(candidate: Candidate) -> discord.Embed:
    embed = discord.Embed(
        title=f"{verdict_emoji(candidate.verdict_rank)} {candidate.company} | {candidate.verdict.title()}",
        description=candidate.title[:4000] if candidate.title else "Найдена новая халява",
        color=EMBED_COLORS[candidate.verdict_rank],
    )

    if candidate.codes:
        embed.add_field(
            name="Промокод",
            value="\n".join(f"`{code}`" for code in candidate.codes),
            inline=False,
        )
    if candidate.offers:
        embed.add_field(name="Оффер", value="\n".join(candidate.offers), inline=False)

    embed.add_field(name="Категория", value=candidate.category, inline=True)
    embed.add_field(name="Источник", value=f"{candidate.source_type} / {candidate.source_name}", inline=True)
    embed.add_field(
        name="Где найден",
        value=candidate.external_url[:1024] if candidate.external_url else "ссылка не указана",
        inline=False,
    )
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
    def __init__(self, config: dict[str, Any], storage: Storage, channel_id: int) -> None:
        intents = discord.Intents.none()
        super().__init__(intents=intents)
        self.config = config
        self.storage = storage
        self.channel_id = channel_id
        self.http_client = httpx.AsyncClient(
            headers={
                "User-Agent": "promo-watcher-bot/1.0 (+discord alert bot)",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self.poll_task: asyncio.Task[None] | None = None
        self.startup_message_sent = False

    async def setup_hook(self) -> None:
        self.poll_task = asyncio.create_task(self.poll_loop())

    async def close(self) -> None:
        if self.poll_task:
            self.poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.poll_task
        await self.http_client.aclose()
        self.storage.close()
        await super().close()

    async def on_ready(self) -> None:
        logging.info("Discord bot connected as %s", self.user)

    async def resolve_channel(self) -> Any:
        channel = self.get_channel(self.channel_id)
        if channel is not None:
            return channel
        return await self.fetch_channel(self.channel_id)

    async def poll_loop(self) -> None:
        await self.wait_until_ready()
        channel = await self.resolve_channel()

        if not self.startup_message_sent:
            await channel.send(
                "Бот запущен и мониторинг активен.\n"
                f"Проверка источников: каждые {self.config['poll_interval_minutes']} мин.\n"
                f"Повтор плохих кодов: каждые {self.config['bad_repeat_minutes']} мин."
            )
            self.startup_message_sent = True

        while not self.is_closed():
            now = datetime.now(UTC)
            try:
                candidates = await collect_candidates(self.http_client, self.config)
                sent_count = 0
                max_messages_per_cycle = self.config["max_messages_per_cycle"]

                for candidate in candidates:
                    if sent_count >= max_messages_per_cycle:
                        logging.info("Cycle send limit reached: limit=%s", max_messages_per_cycle)
                        break
                    if not self.storage.should_send(candidate, now, self.config["bad_repeat_minutes"]):
                        continue

                    await channel.send(embed=candidate_to_embed(candidate))
                    self.storage.mark_sent(candidate, now)
                    sent_count += 1
                    await asyncio.sleep(1.0)

                logging.info("Polling cycle completed: checked=%s sent=%s", len(candidates), sent_count)
            except Exception as error:  # noqa: BLE001
                logging.exception("Polling cycle failed: %s", error)

            await asyncio.sleep(self.config["poll_interval_minutes"] * 60)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def start_health_server() -> None:
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logging.info("Health server listening on port %s", port)


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    config_path = Path(os.environ.get("APP_CONFIG_PATH", "config.json"))
    config = load_config(config_path)
    storage = Storage(Path("data") / "sent_candidates.sqlite3")

    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")

    channel_id_raw = os.environ.get("DISCORD_CHANNEL_ID")
    if not channel_id_raw:
        raise RuntimeError("DISCORD_CHANNEL_ID is required")
    try:
        channel_id = int(channel_id_raw)
    except ValueError as error:
        raise RuntimeError("DISCORD_CHANNEL_ID must be an integer") from error

    start_health_server()
    client = PromoWatcherClient(config=config, storage=storage, channel_id=channel_id)
    client.run(token, log_handler=None)


if __name__ == "__main__":
    main()
