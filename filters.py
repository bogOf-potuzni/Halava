import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Any

from bs4 import BeautifulSoup

UTC = timezone.utc

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
    "free", "coupon", "cupon", "code", "promo", "promocode", "worked", "working",
    "redeem", "gift", "invite", "trial", "credits", "credit", "discount",
    "voucher", "referral", "bonus", "share", "claim",
)
STRONG_SIGNAL_TERMS = (
    "free coupon", "free cupon", "promo code", "working code", "here is code",
    "use this code", "redeem code", "gift code", "invite code", "student offer",
    "trial works", "credits added",
)
NEGATIVE_SIGNAL_TERMS = (
    "expired", "doesn't work", "doesnt work", "not working", "dead code", "fake",
    "scam", "patched", "cracked", "bypass", "leak",
)
LEGAL_STOP_TERMS = (
    "court", "courtroom", "lawsuit", "hearing", "judge", "trial begins",
    "legal battle", "legal fight", "case against", "musk openai trial",
)
EFFECT_PATTERNS = (
    r"(\d+\s*(?:months?|days?|weeks?)\s+(?:free|trial))",
    r"(\d+\s*(?:credits?|tokens?))",
    r"(\d+%\s*(?:off|discount))",
    r"(free\s+trial)",
    r"(student\s+offer)",
    r"(gift\s+code)",
)


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
    rejected_legal_noise: int = 0
    accepted: int = 0


@dataclass(slots=True)
class CycleStats:
    scanned_sources: int
    source_stats: list[SourceStats]
    total_entries: int
    accepted_candidates: int
    rejected_no_signal: int
    rejected_too_old: int
    rejected_legal_noise: int
    category_counts: dict[int, int]


def normalize_text(value: str) -> str:
    return " ".join(unescape(value or "").split())


def html_to_text(value: str) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "lxml")
    return normalize_text(soup.get_text(" ", strip=True))


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


def is_legal_noise(text: str, codes: list[str], signals: list[str]) -> bool:
    lowered = text.lower()
    if codes:
        return False
    if any(term in lowered for term in LEGAL_STOP_TERMS):
        return True
    return signals == ["trial", "company-match"] or signals == ["company-match", "trial"]


def classify_candidate(
    text: str,
    company: str,
    published_at: datetime,
    now: datetime,
    config: dict[str, Any],
    codes: list[str],
    signals: list[str],
    source_type: str,
    source_bonus: int,
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
    elif source_type == "news":
        score -= 1

    score += source_bonus

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


def should_replace_candidate(current: Candidate | None, new: Candidate) -> bool:
    if current is None:
        return True
    if new.verdict_rank < current.verdict_rank:
        return True
    return new.verdict_rank == current.verdict_rank and new.published_at > current.published_at


def process_feed_entries(parsed: Any, source: dict[str, Any], now: datetime, config: dict[str, Any]) -> tuple[list[Candidate], SourceStats]:
    entries = getattr(parsed, "entries", [])
    source_stats = SourceStats(name=source["name"], total_entries=len(entries))
    candidates: list[Candidate] = []

    logging.info("Scanning source %s: entries=%s", source["name"], len(entries))
    for entry in entries:
        published_at = source["parse_datetime"](entry)
        if published_at is None:
            source_stats.rejected_no_signal += 1
            continue

        title = normalize_text(entry.get("title", ""))
        summary = html_to_text(entry.get("summary", ""))
        text = f"{title} {summary}".strip()
        aliases = config["company_aliases"].get(source["company"], [])
        codes = extract_codes(text)
        signals = extract_signals(text, aliases)

        if not codes and len(signals) < 2:
            source_stats.rejected_no_signal += 1
            continue
        if is_legal_noise(text, codes, signals):
            source_stats.rejected_legal_noise += 1
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
            source_bonus=int(source.get("source_bonus", 0)),
        )
        if not source_age_allowed(verdict_rank, published_at, now, config):
            source_stats.rejected_too_old += 1
            continue

        candidates.append(
            Candidate(
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
        )
        source_stats.accepted += 1

    logging.info(
        "Source %s summary: entries=%s accepted=%s rejected_no_signal=%s rejected_legal_noise=%s rejected_too_old=%s",
        source_stats.name,
        source_stats.total_entries,
        source_stats.accepted,
        source_stats.rejected_no_signal,
        source_stats.rejected_legal_noise,
        source_stats.rejected_too_old,
    )
    return candidates, source_stats
