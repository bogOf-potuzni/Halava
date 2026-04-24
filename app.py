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

CODE_PATTERN = re.compile(r"\b[A-Z0-9]{4,28}(?:-[A-Z0-9]{2,12}){0,4}\b", re.IGNORECASE)
MIXED_CODE_PATTERN = re.compile(r"\b(?=[A-Z0-9-]{6,28}\b)(?=.*[A-Z])(?=.*\d)[A-Z0-9-]+\b", re.IGNORECASE)
COMMON_WORDS = {
    "ABOUT", "ACCESS", "ACCOUNT", "ACTIVE", "AI", "APRIL", "AUDIO", "AVAILABLE", "BONUS",
    "BRANDS", "BUSINESS", "CHATGPT", "CLAUDE", "CODE", "CODES", "CREDIT", "CREDITS", "DAY",
    "DAYS", "DEAL", "DISCOUNT", "FREE", "FROM", "GIFT", "GPT", "GUIDE", "HELLO", "HOURS",
    "INVITE", "MONTH", "MONTHS", "NEWS", "NOW", "OFFER", "OPENAI", "PLAN", "PLUS", "POST",
    "PROMO", "PROMOCODE", "REDEEM", "RUNWAY", "SALE", "SAVE", "STUDENT", "SUBSCRIPTION",
    "THIS", "TODAY", "TRIAL", "VIDEO", "WITH", "WORKED", "WORKING", "YEAR", "YEARS",
}
DIRTY_SIGNAL_TERMS = (
    "free",
    "coupon",
    "cupon",
    "code",
    "promo",
    "promocode",
    "worked",
    "working",
    "redeem",
    "gift",
    "invite",
    "trial",
    "credits",
    "credit",
    "discount",
    "voucher",
    "referral",
    "bonus",
    "share",
    "claim",
)
STRONG_SIGNAL_TERMS = (
    "free coupon",
    "free cupon",
    "promo code",
    "working code",
    "here is code",
    "use this code",
    "redeem code",
    "gift code",
    "invite code",
    "student offer",
    "trial works",
    "credits added",
)
NEGATIVE_SIGNAL_TERMS = (
    "expired",
    "doesn't work",
    "doesnt work",
    "not working",
    "dead code",
    "fake",
    "scam",
    "patched",
    "cracked",
    "bypass",
    "leak",
)
EFFECT_PATTERNS = (
    r"(\d+\s*(?:months?|days?|weeks?)\s+(?:free|trial))",
    r"(\d+\s*(?:credits?|tokens?))",
    r"(\d+%\s*(?:off|discount))",
    r"(free\s+trial)",
    r"(student\s+offer)",
    r"(gift\s+code)",
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
    signals: list[str]
    verdict: str
    verdict_rank: int
    confidence_label: str
    estimated_lifetime: str
    activation_risk: str
    availability_type: str
    estimated_effect: str
    popularity_label: str
    identity_key: str


@dataclass(slots=True)
class SourceStats:
    name: str
    total_entries: int = 0
    rejected_no_signal: int = 0
    rejected_too_old: int = 0
    accepted: int = 0


@dataclass(slots=True)
class CycleStats:
    scanned_sources: int
    source_stats: list[SourceStats]
    total_entries: int
    accepted_candidates: int
    rejected_no_signal: int
    rejected_too_old: int
    category_counts: dict[int, int]


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
                payload_preview TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def should_send(
        self,
        candidate: Candidate,
        now: datetime,
        medium_repeat_minutes: int,
        low_repeat_minutes: int,
    ) -> bool:
        row = self.connection.execute(
            "SELECT verdict_rank, sent_at FROM sent_candidates WHERE identity_key = ?",
            (candidate.identity_key,),
        ).fetchone()
        if row is None:
            return True

        previous_rank = int(row["verdict_rank"])
        if candidate.verdict_rank < previous_rank:
            return True
        if candidate.verdict_rank in {1, 2}:
            return False

        sent_at = datetime.fromisoformat(row["sent_at"])
        repeat_minutes = medium_repeat_minutes if candidate.verdict_rank == 3 else low_repeat_minutes
        return now - sent_at >= timedelta(minutes=repeat_minutes)

    def mark_sent(self, candidate: Candidate, now: datetime) -> None:
        preview = ",".join(candidate.codes or candidate.signals or [candidate.title[:64]])
        self.connection.execute(
            """
            INSERT INTO sent_candidates (identity_key, verdict_rank, sent_at, published_at, payload_preview)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(identity_key) DO UPDATE SET
                verdict_rank = excluded.verdict_rank,
                sent_at = excluded.sent_at,
                published_at = excluded.published_at,
                payload_preview = excluded.payload_preview
            """,
            (
                candidate.identity_key,
                candidate.verdict_rank,
                now.isoformat(),
                candidate.published_at.isoformat(),
                preview,
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def load_config(config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    required_keys = {
        "poll_interval_minutes",
        "max_best_post_age_hours",
        "max_good_post_age_hours",
        "max_medium_post_age_hours",
        "max_low_post_age_hours",
        "max_bad_post_age_hours",
        "medium_repeat_minutes",
        "low_repeat_minutes",
        "max_messages_per_cycle",
        "sources",
        "keyword_groups",
        "company_aliases",
    }
    missing = sorted(required_keys - set(config))
    if missing:
        raise RuntimeError(f"Missing config keys: {', '.join(missing)}")


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
    seen: set[str] = set()
    codes: list[str] = []
    for pattern in (MIXED_CODE_PATTERN, CODE_PATTERN):
        for match in pattern.findall(text.upper()):
            raw = match.strip("-")
            digit_count = sum(character.isdigit() for character in raw)
            letter_count = sum(character.isalpha() for character in raw)
            compact = raw.replace("-", "")
            if raw in COMMON_WORDS:
                continue
            if len(compact) < 6:
                continue
            if digit_count == 0:
                continue
            if digit_count == 1 and letter_count < 6:
                continue
            if raw in seen:
                continue
            seen.add(raw)
            codes.append(raw)
    return codes[:5]


def extract_signals(text: str, company_aliases: list[str]) -> list[str]:
    lowered = text.lower()
    signals: list[str] = []
    for term in STRONG_SIGNAL_TERMS:
        if term in lowered:
            signals.append(term)
    for term in DIRTY_SIGNAL_TERMS:
        if term in lowered and term not in signals:
            signals.append(term)
    if any(alias in lowered for alias in company_aliases):
        signals.append("company-match")
    return signals[:8]


def estimate_effect(text: str) -> str:
    lowered = text.lower()
    for pattern in EFFECT_PATTERNS:
        match = re.search(pattern, lowered)
        if match:
            return match.group(1)
    return "неясно"


def popularity_label(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("viral", "trending", "hot", "popular", "blowing up")):
        return "высокая"
    if any(term in lowered for term in ("comments", "upvotes", "likes", "shared", "reply")):
        return "средняя"
    return "неясно"


def hours_to_text(min_hours: int, max_hours: int | None) -> str:
    if max_hours is None:
        return f"{min_hours}+ ч"
    return f"{min_hours}-{max_hours} ч"


def source_age_allowed(rank: int, published_at: datetime, now: datetime, config: dict[str, Any]) -> bool:
    age_hours = (now - published_at).total_seconds() / 3600
    limits = {
        1: config["max_best_post_age_hours"],
        2: config["max_good_post_age_hours"],
        3: config["max_medium_post_age_hours"],
        4: config["max_low_post_age_hours"],
        5: config["max_bad_post_age_hours"],
    }
    return age_hours <= limits.get(rank, 0)


def classify_candidate(
    text: str,
    company: str,
    published_at: datetime,
    now: datetime,
    config: dict[str, Any],
    codes: list[str],
    signals: list[str],
    source_type: str,
) -> tuple[int, str, str, str, str, str]:
    lowered = text.lower()
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600)
    score = 0

    positive_hits = sum(1 for word in config["keyword_groups"]["positive"] if word in lowered)
    negative_hits = sum(1 for word in config["keyword_groups"]["negative"] if word in lowered)
    score += positive_hits * 2
    score -= negative_hits * 4

    aliases = config["company_aliases"].get(company, [])
    if any(alias in lowered for alias in aliases):
        score += 3

    strong_signal_hits = sum(1 for term in STRONG_SIGNAL_TERMS if term in lowered)
    dirty_signal_hits = sum(1 for term in DIRTY_SIGNAL_TERMS if term in lowered)
    bad_signal_hits = sum(1 for term in NEGATIVE_SIGNAL_TERMS if term in lowered)

    score += strong_signal_hits * 3
    score += min(dirty_signal_hits, 4)
    score -= bad_signal_hits * 4

    if codes:
        score += 5
        if any(len(code) >= 8 for code in codes):
            score += 2
    if signals:
        score += min(len(signals), 4)

    if source_type == "reddit":
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
        score -= 6

    if "official" in lowered:
        score += 3
    if "referral" in lowered:
        score -= 1

    if score >= 15:
        return 1, "лучший", "высокая", hours_to_text(1, 3), "низкий", "массовый"
    if score >= 11:
        return 2, "хороший", "выше средней", hours_to_text(1, 4), "средний", "ограниченный"
    if score >= 8:
        return 3, "средний", "средняя", hours_to_text(1, 6), "средний", "ограниченный"
    if score >= 5:
        return 4, "ниже среднего", "ниже средней", hours_to_text(1, 4), "высокий", "неясно"
    return 5, "плохой", "низкая", hours_to_text(0, 2), "очень высокий", "неясно"


def build_identity_key(company: str, codes: list[str], signals: list[str], title: str, summary: str) -> str:
    if codes:
        payload = "/".join(sorted(code.upper() for code in codes))
    elif signals:
        payload = "/".join(sorted(signal.lower() for signal in signals))
    else:
        payload = normalize_text(f"{title} {summary}").lower()[:160]
    return hashlib.sha256(f"{company.lower()}|{payload}".encode("utf-8")).hexdigest()


async def fetch_feed(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url, timeout=20.0, follow_redirects=True)
    response.raise_for_status()
    return feedparser.parse(response.text)


def should_replace_candidate(current: Candidate | None, new: Candidate) -> bool:
    if current is None:
        return True
    if new.verdict_rank < current.verdict_rank:
        return True
    return new.verdict_rank == current.verdict_rank and new.published_at > current.published_at


async def collect_candidates(client: httpx.AsyncClient, config: dict[str, Any]) -> tuple[list[Candidate], CycleStats]:
    now = datetime.now(UTC)
    results = await asyncio.gather(
        *(fetch_feed(client, source["url"]) for source in config["sources"]),
        return_exceptions=True,
    )

    unique_candidates: dict[str, Candidate] = {}
    source_stats_list: list[SourceStats] = []
    total_entries = 0
    total_rejected_no_signal = 0
    total_rejected_too_old = 0
    category_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    for source, parsed in zip(config["sources"], results):
        if isinstance(parsed, Exception):
            logging.warning("Source fetch failed for %s: %s", source["name"], parsed)
            continue

        entries = getattr(parsed, "entries", [])
        source_stats = SourceStats(name=source["name"], total_entries=len(entries))
        source_stats_list.append(source_stats)
        total_entries += len(entries)
        logging.info("Scanning source %s: entries=%s", source["name"], len(entries))
        for entry in entries:
            published_at = parse_datetime(entry)
            if published_at is None:
                source_stats.rejected_no_signal += 1
                total_rejected_no_signal += 1
                continue

            title = normalize_text(entry.get("title", ""))
            summary = html_to_text(entry.get("summary", ""))
            text = f"{title} {summary}".strip()
            aliases = config["company_aliases"].get(source["company"], [])
            codes = extract_codes(text)
            signals = extract_signals(text, aliases)
            if not codes and len(signals) < 2:
                source_stats.rejected_no_signal += 1
                total_rejected_no_signal += 1
                continue

            verdict_rank, verdict, confidence, lifetime, risk, availability = classify_candidate(
                text=text,
                company=source["company"],
                published_at=published_at,
                now=now,
                config=config,
                codes=codes,
                signals=signals,
                source_type=source["source_type"],
            )
            if not source_age_allowed(verdict_rank, published_at, now, config):
                source_stats.rejected_too_old += 1
                total_rejected_too_old += 1
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
                signals=signals[:6],
                verdict=verdict,
                verdict_rank=verdict_rank,
                confidence_label=confidence,
                estimated_lifetime=lifetime,
                activation_risk=risk,
                availability_type=availability,
                estimated_effect=estimate_effect(text),
                popularity_label=popularity_label(text),
                identity_key=build_identity_key(source["company"], codes[:3], signals[:6], title, summary),
            )

            current = unique_candidates.get(candidate.identity_key)
            if should_replace_candidate(current, candidate):
                unique_candidates[candidate.identity_key] = candidate
            source_stats.accepted += 1
            category_counts[verdict_rank] += 1

        logging.info(
            "Source %s summary: entries=%s accepted=%s rejected_no_signal=%s rejected_too_old=%s",
            source_stats.name,
            source_stats.total_entries,
            source_stats.accepted,
            source_stats.rejected_no_signal,
            source_stats.rejected_too_old,
        )

    candidates = sorted(unique_candidates.values(), key=lambda item: (item.verdict_rank, -item.published_at.timestamp()))
    stats = CycleStats(
        scanned_sources=len(source_stats_list),
        source_stats=source_stats_list,
        total_entries=total_entries,
        accepted_candidates=len(candidates),
        rejected_no_signal=total_rejected_no_signal,
        rejected_too_old=total_rejected_too_old,
        category_counts=category_counts,
    )
    return candidates, stats


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
        embed.add_field(name="Промокод", value="\n".join(f"`{code}`" for code in candidate.codes), inline=False)
    if candidate.signals:
        embed.add_field(name="Сигналы", value="\n".join(candidate.signals), inline=False)
    embed.add_field(name="Категория", value=candidate.category, inline=True)
    embed.add_field(name="Источник", value=f"{candidate.source_type} / {candidate.source_name}", inline=True)
    embed.add_field(name="Где найден", value=candidate.external_url[:1024] if candidate.external_url else "ссылка не указана", inline=False)
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
        super().__init__(intents=discord.Intents.none())
        self.config = config
        self.storage = storage
        self.channel_id = channel_id
        self.http_client = httpx.AsyncClient(
            headers={
                "User-Agent": "promo-watcher-bot/2.0 (+discord alert bot)",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self.poll_task: asyncio.Task[None] | None = None
        self.startup_message_sent = False
        self.hourly_started_at = datetime.now(UTC)
        self.hourly_entries = 0
        self.hourly_candidates = 0
        self.hourly_sent = 0
        self.hourly_rejected_no_signal = 0
        self.hourly_rejected_too_old = 0

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

    async def resolve_channel(self) -> Any:
        channel = self.get_channel(self.channel_id)
        if channel is not None:
            return channel
        return await self.fetch_channel(self.channel_id)

    async def poll_loop(self) -> None:
        await self.wait_until_ready()
        channel = await self.resolve_channel()

        if not self.startup_message_sent:
            await channel.send("Бот запущен")
            self.startup_message_sent = True

        while not self.is_closed():
            now = datetime.now(UTC)
            try:
                logging.info(
                    "Starting scan: sources=%s interval=%s min",
                    len(self.config["sources"]),
                    self.config["poll_interval_minutes"],
                )
                candidates, cycle_stats = await collect_candidates(self.http_client, self.config)
                sent_count = 0
                for candidate in candidates:
                    if sent_count >= self.config["max_messages_per_cycle"]:
                        break
                    if not self.storage.should_send(
                        candidate,
                        now,
                        self.config["medium_repeat_minutes"],
                        self.config["low_repeat_minutes"],
                    ):
                        continue
                    await channel.send(embed=candidate_to_embed(candidate))
                    self.storage.mark_sent(candidate, now)
                    sent_count += 1
                    await asyncio.sleep(1.0)
                self.hourly_entries += cycle_stats.total_entries
                self.hourly_candidates += cycle_stats.accepted_candidates
                self.hourly_sent += sent_count
                self.hourly_rejected_no_signal += cycle_stats.rejected_no_signal
                self.hourly_rejected_too_old += cycle_stats.rejected_too_old
                logging.info(
                    "Polling cycle completed: sources=%s entries=%s checked=%s rejected_no_signal=%s rejected_too_old=%s sent=%s categories=1:%s 2:%s 3:%s 4:%s 5:%s",
                    cycle_stats.scanned_sources,
                    cycle_stats.total_entries,
                    cycle_stats.accepted_candidates,
                    cycle_stats.rejected_no_signal,
                    cycle_stats.rejected_too_old,
                    sent_count,
                    cycle_stats.category_counts[1],
                    cycle_stats.category_counts[2],
                    cycle_stats.category_counts[3],
                    cycle_stats.category_counts[4],
                    cycle_stats.category_counts[5],
                )
                if now - self.hourly_started_at >= timedelta(hours=1):
                    await channel.send(
                        "Часовой отчет\n"
                        f"Просканировано записей: {self.hourly_entries}\n"
                        f"Прошло фильтр: {self.hourly_candidates}\n"
                        f"Отправлено: {self.hourly_sent}\n"
                        f"Отклонено без сигналов: {self.hourly_rejected_no_signal}\n"
                        f"Отклонено как старое: {self.hourly_rejected_too_old}"
                    )
                    self.hourly_started_at = now
                    self.hourly_entries = 0
                    self.hourly_candidates = 0
                    self.hourly_sent = 0
                    self.hourly_rejected_no_signal = 0
                    self.hourly_rejected_too_old = 0
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

    config = load_config(Path(os.environ.get("APP_CONFIG_PATH", "config.json")))
    storage = Storage(Path("data") / "sent_candidates.sqlite3")

    token = os.environ.get("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")

    channel_id_raw = os.environ.get("DISCORD_CHANNEL_ID")
    if not channel_id_raw:
        raise RuntimeError("DISCORD_CHANNEL_ID is required")

    start_health_server()
    client = PromoWatcherClient(config=config, storage=storage, channel_id=int(channel_id_raw))
    client.run(token, log_handler=None)


if __name__ == "__main__":
    main()
