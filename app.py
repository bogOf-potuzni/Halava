import asyncio
import contextlib
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import discord
import feedparser
import httpx
from dotenv import load_dotenv

from filters import (
    Candidate,
    CycleStats,
    SourceStats,
    build_identity_key,
    process_feed_entries,
)

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))
EMBED_COLORS = {
    1: 0x2ECC71,
    2: 0x3498DB,
    3: 0xF1C40F,
    4: 0xE67E22,
    5: 0xE74C3C,
}


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
    return config


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
    enriched_sources = [
        {
            **source,
            "parse_datetime": parse_datetime,
        }
        for source in config["sources"]
    ]
    results = await asyncio.gather(
        *(fetch_feed(client, source["url"]) for source in enriched_sources),
        return_exceptions=True,
    )

    unique_candidates: dict[str, Candidate] = {}
    source_stats_list: list[SourceStats] = []
    total_entries = 0
    total_rejected_no_signal = 0
    total_rejected_too_old = 0
    total_rejected_legal_noise = 0
    category_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    for source, parsed in zip(enriched_sources, results):
        if isinstance(parsed, Exception):
            logging.warning("Source fetch failed for %s: %s", source["name"], parsed)
            continue

        candidates, source_stats = process_feed_entries(parsed, source, now, config)
        source_stats_list.append(source_stats)
        total_entries += source_stats.total_entries
        total_rejected_no_signal += source_stats.rejected_no_signal
        total_rejected_too_old += source_stats.rejected_too_old
        total_rejected_legal_noise += source_stats.rejected_legal_noise

        for candidate in candidates:
            current = unique_candidates.get(candidate.identity_key)
            if should_replace_candidate(current, candidate):
                unique_candidates[candidate.identity_key] = candidate

    for candidate in unique_candidates.values():
        category_counts[candidate.verdict_rank] += 1

    candidates = sorted(unique_candidates.values(), key=lambda item: (item.verdict_rank, -item.published_at.timestamp()))
    stats = CycleStats(
        scanned_sources=len(source_stats_list),
        source_stats=source_stats_list,
        total_entries=total_entries,
        accepted_candidates=len(candidates),
        rejected_no_signal=total_rejected_no_signal,
        rejected_too_old=total_rejected_too_old,
        rejected_legal_noise=total_rejected_legal_noise,
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
                "User-Agent": "promo-watcher-bot/3.0 (+discord alert bot)",
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
        self.hourly_rejected_legal_noise = 0

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
                logging.info("Starting scan: sources=%s interval=%s min", len(self.config["sources"]), self.config["poll_interval_minutes"])
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
                self.hourly_rejected_legal_noise += cycle_stats.rejected_legal_noise

                logging.info(
                    "Polling cycle completed: sources=%s entries=%s checked=%s rejected_no_signal=%s rejected_legal_noise=%s rejected_too_old=%s sent=%s categories=1:%s 2:%s 3:%s 4:%s 5:%s",
                    cycle_stats.scanned_sources,
                    cycle_stats.total_entries,
                    cycle_stats.accepted_candidates,
                    cycle_stats.rejected_no_signal,
                    cycle_stats.rejected_legal_noise,
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
                        f"Отклонено по legal/news шуму: {self.hourly_rejected_legal_noise}\n"
                        f"Отклонено как старое: {self.hourly_rejected_too_old}"
                    )
                    self.hourly_started_at = now
                    self.hourly_entries = 0
                    self.hourly_candidates = 0
                    self.hourly_sent = 0
                    self.hourly_rejected_no_signal = 0
                    self.hourly_rejected_too_old = 0
                    self.hourly_rejected_legal_noise = 0
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
