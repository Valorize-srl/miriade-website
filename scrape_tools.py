#!/usr/bin/env python3
"""
Async scraper: rileva tool di email marketing nei siti delle concessionarie.
Legge concessionarie_verde_giallo.csv, visita ogni dominio, cerca pattern noti
nel sorgente HTML e salva i risultati in concessionarie_tool_rilevato.csv.
"""

import asyncio
import csv
import time
import sys
from collections import Counter

import aiohttp

# ── Tool patterns ──────────────────────────────────────────────────────────
TOOLS = {
    "Mailup": ["mailup.net", "mlsend.com", "mailup.com", "mludmailpro", "r.mailup"],
    "Esendex": ["esendex.com", "esendex.it", "esendex.net"],
    "Mailchimp": ["mailchimp.com", "chimpstatic.com", "list-manage.com"],
    "Brevo": ["brevo.com", "sendinblue.com"],
    "Klaviyo": ["klaviyo.com"],
    "ActiveCampaign": ["activecampaign.com"],
    "Hubspot": ["hubspot.com", "hs-scripts.com"],
}

CONCURRENCY = 40
TIMEOUT = 10  # seconds per domain
INPUT_FILE = "concessionarie_verde_giallo.csv"
OUTPUT_FILE = "concessionarie_tool_rilevato.csv"


def detect_tools(html: str) -> list[str]:
    """Return list of tool names found in the HTML source."""
    html_lower = html.lower()
    found = []
    for tool_name, patterns in TOOLS.items():
        if any(p in html_lower for p in patterns):
            found.append(tool_name)
    return found


async def fetch_domain(session: aiohttp.ClientSession, domain: str) -> tuple[str, str]:
    """Try https then http. Return (tools_found, status)."""
    domain = domain.strip().lower()
    if not domain:
        return ("", "dominio_vuoto")

    # Remove protocol if someone left it in the CSV
    for prefix in ("https://", "http://", "www."):
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
    domain = domain.rstrip("/")

    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                html = await resp.text(errors="replace")
                tools = detect_tools(html)
                tool_str = ", ".join(tools) if tools else ""
                return (tool_str, f"{resp.status}")
        except Exception:
            continue

    return ("", "unreachable")


async def main():
    # ── Read CSV ───────────────────────────────────────────────────────────
    with open(INPUT_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    if "dominio" not in fieldnames:
        print(f"ERRORE: colonna 'dominio' non trovata. Colonne: {fieldnames}")
        sys.exit(1)

    total = len(rows)
    print(f"Caricate {total} righe. Avvio scraping con {CONCURRENCY} connessioni parallele...\n")

    # ── Scrape ─────────────────────────────────────────────────────────────
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0
    start = time.time()

    async def process(idx: int, row: dict):
        nonlocal done
        async with sem:
            domain = row.get("dominio", "")
            tool_str, status = await fetch_domain(session, domain)
            row["tool_rilevato"] = tool_str
            row["status"] = status
            done += 1
            if done % 200 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed else 0
                print(f"  [{done}/{total}] {rate:.0f} domini/s  elapsed {elapsed:.0f}s")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DealershipScraper/1.0)"},
    ) as session:
        tasks = [process(i, row) for i, row in enumerate(rows)]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start
    print(f"\nScraping completato in {elapsed:.0f}s")

    # ── Sort: rows with tool first ─────────────────────────────────────────
    rows.sort(key=lambda r: (0 if r["tool_rilevato"] else 1, r.get("dominio", "")))

    # ── Write output CSV ───────────────────────────────────────────────────
    out_fields = list(fieldnames) + ["tool_rilevato", "status"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Risultati salvati in {OUTPUT_FILE}\n")

    # ── Breakdown ──────────────────────────────────────────────────────────
    tool_counter = Counter()
    with_tool = 0
    for row in rows:
        tools = row["tool_rilevato"]
        if tools:
            with_tool += 1
            for t in tools.split(", "):
                tool_counter[t] += 1

    print("=" * 50)
    print(f"BREAKDOWN – Tool rilevati su {total} concessionarie")
    print("=" * 50)
    for tool_name, count in tool_counter.most_common():
        pct = count / total * 100
        print(f"  {tool_name:<20s} {count:>5d}  ({pct:.1f}%)")
    print(f"  {'─' * 40}")
    print(f"  {'Con almeno 1 tool':<20s} {with_tool:>5d}  ({with_tool/total*100:.1f}%)")
    print(f"  {'Nessun tool':<20s} {total - with_tool:>5d}  ({(total-with_tool)/total*100:.1f}%)")

    # Status breakdown
    status_counter = Counter(r["status"] for r in rows)
    print(f"\n{'STATUS BREAKDOWN':}")
    for st, count in status_counter.most_common():
        print(f"  {st:<20s} {count:>5d}")


if __name__ == "__main__":
    asyncio.run(main())
