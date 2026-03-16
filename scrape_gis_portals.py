#!/usr/bin/env python3
"""
GIS Portal Scraper — Discovers new county/state parcel data endpoints
by crawling known GIS directories, state portal listings, and search engines.

Uses Scrapling for HTML scraping, httpx for REST APIs.
Deduplicates against existing parcel_sources.json before adding.

Usage:
    python scrape_gis_portals.py [--verify] [--push]
"""

import httpx
import json
import re
import sys
import time
import hashlib
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

from scrapling.fetchers import Fetcher, FetcherSession

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent.parent / "data"
EXISTING_JSON = DATA_DIR / "parcel_sources.json"
NEW_JSON = DATA_DIR / "new_discoveries.json"
COMBINED_JSON = DATA_DIR / "parcel_sources.json"
COMBINED_MD = DATA_DIR / "PARCEL_DATA_SOURCES.md"

# All 50 states
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

# ─────────────────────────────────────────────────────────────────────────────
# Known GIS directories and portals to scrape for links
# ─────────────────────────────────────────────────────────────────────────────

# These are pages that LIST county GIS sites — we scrape them for links
GIS_DIRECTORY_PAGES = [
    # National directories of county GIS
    "https://www.countygis.com/",
    "https://www.nsgic.org/resources/gis-inventory",
    "https://hub.arcgis.com/search?collection=Dataset&q=parcels&type=Feature+Service",

    # State GIS clearinghouses
    "https://alabamagis.com/",
    "https://www.asgdc.state.al.us/",
    "https://gis.arkansas.gov/product/parcels/",
    "https://gis.data.ca.gov/",
    "https://geodata.co.gov/",
    "https://ct-deep-gis-open-data-website-ctdeep.hub.arcgis.com/",
    "https://firstmap.delaware.gov/",
    "https://www.fgdl.org/",  # Florida
    "https://data.georgiaspatial.org/",
    "https://planning.hawaii.gov/gis/",
    "https://gis.idaho.gov/",
    "https://clearinghouse.isgs.illinois.edu/",
    "https://maps.indiana.edu/layerGallery.html",
    "https://geodata.iowa.gov/",
    "https://www.kansasgis.org/",
    "https://kygeonet.ky.gov/",
    "https://atlas.ga.lsu.edu/",  # Louisiana
    "https://www.maine.gov/geolib/catalog.html",
    "https://data.imap.maryland.gov/",
    "https://www.mass.gov/info-details/massgis-data-layers",
    "https://gis-michigan.opendata.arcgis.com/",
    "https://gisdata.mn.gov/",
    "https://www.maris.state.ms.us/",
    "https://www.msdis.missouri.edu/",
    "https://svc.mt.gov/msl/mtcadastral",
    "https://www.nebraskamap.gov/",
    "https://data-nrrd.opendata.arcgis.com/",  # Nevada
    "https://granit.unh.edu/",  # New Hampshire
    "https://njgin.nj.gov/",
    "https://rgis.unm.edu/",
    "https://gis.ny.gov/",
    "https://www.nconemap.gov/",
    "https://gis.nd.gov/",
    "https://ogrip.oit.ohio.gov/",
    "https://data.ok.gov/",
    "https://geo.oregon.gov/",
    "https://www.pasda.psu.edu/",
    "https://www.rigis.org/",  # Rhode Island
    "https://www.gis.sc.gov/",
    "https://opendata2017-09-18t192802468z-sdbit.hub.arcgis.com/",  # South Dakota
    "https://www.tn.gov/finance/sts-gis.html",
    "https://tnmap.tn.gov/",
    "https://data.tnris.org/",  # Texas
    "https://gis.utah.gov/",
    "https://geodata.vermont.gov/",
    "https://vgin.vdem.virginia.gov/",
    "https://geo.wa.gov/",
    "https://wvgis.wvu.edu/",
    "https://data-ltsb.opendata.arcgis.com/",
    "https://wyogeo.wygisc.org/",
]

# Direct ArcGIS Hub search pages (these list datasets)
ARCGIS_HUB_SEARCHES = [
    "https://hub.arcgis.com/search?collection=Dataset&q=parcels&type=Feature+Service&sort=-numviews",
    "https://hub.arcgis.com/search?collection=Dataset&q=tax+parcels&type=Feature+Service",
    "https://hub.arcgis.com/search?collection=Dataset&q=zoning&type=Feature+Service",
    "https://hub.arcgis.com/search?collection=Dataset&q=land+use&type=Feature+Service",
    "https://hub.arcgis.com/search?collection=Dataset&q=property+boundaries&type=Feature+Service",
    "https://hub.arcgis.com/search?collection=Dataset&q=assessor+parcels&type=Feature+Service",
]

# County-level ArcGIS REST endpoints to probe (common patterns)
COUNTY_ARCGIS_PATTERNS = [
    # {county} and {state} will be substituted
    "https://gis.{county}county{state_abbr}.gov/arcgis/rest/services",
    "https://gis.{county}county.gov/arcgis/rest/services",
    "https://maps.{county}county.gov/arcgis/rest/services",
    "https://arcgis.{county}county.gov/arcgis/rest/services",
    "https://gis.{county}county{state_abbr}.us/arcgis/rest/services",
    "https://{county}county.maps.arcgis.com/",
    "https://experience.arcgis.com/experience/{county}county",
]


def load_existing() -> dict:
    """Load existing parcel sources to check for duplicates."""
    if EXISTING_JSON.exists():
        with open(EXISTING_JSON) as f:
            return json.load(f)
    return {}


def url_fingerprint(url: str) -> str:
    """Normalize URL to a fingerprint for dedup."""
    if not url:
        return ""
    parsed = urlparse(url.lower().rstrip("/"))
    # Normalize: strip www, trailing slashes, query params for dedup
    host = parsed.netloc.replace("www.", "")
    path = parsed.path.rstrip("/")
    return hashlib.md5(f"{host}{path}".encode()).hexdigest()


def extract_gis_links(html_content: str, base_url: str) -> list[dict]:
    """Extract GIS-related links from an HTML page."""
    from scrapling.parser import Selector

    results = []
    try:
        page = Selector(html_content)
    except Exception:
        return results

    # Find all links
    for link in page.css("a"):
        href = link.attrib.get("href", "")
        text = (link.text or "").strip()

        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        # Resolve relative URLs
        full_url = urljoin(base_url, href)

        # Filter for GIS/parcel related links
        url_lower = full_url.lower()
        text_lower = text.lower()
        combined = f"{url_lower} {text_lower}"

        gis_keywords = [
            "parcel", "parcels", "gis", "arcgis", "mapserver", "featureserver",
            "property", "assessor", "cadastral", "tax map", "land use",
            "zoning", "plat", "lot line", "boundary", "boundaries",
        ]

        if any(kw in combined for kw in gis_keywords):
            # Extra filter: skip obvious non-data links
            skip_keywords = ["login", "signup", "contact", "about", "help",
                           "faq", "privacy", "terms", "twitter", "facebook",
                           "linkedin", "youtube", ".pdf", ".doc"]
            if any(sk in url_lower for sk in skip_keywords):
                continue

            results.append({
                "url": full_url,
                "title": text[:200] if text else "",
                "source_page": base_url,
            })

    return results


def scrape_directory_pages(session) -> list[dict]:
    """Scrape known GIS directory pages for links to parcel data."""
    all_links = []

    for i, url in enumerate(GIS_DIRECTORY_PAGES):
        print(f"  [{i+1}/{len(GIS_DIRECTORY_PAGES)}] {url[:70]}...", end=" ", flush=True)
        try:
            page = session.get(url, stealthy_headers=True)
            if page.status == 200:
                links = extract_gis_links(page.body, url)
                all_links.extend(links)
                print(f"→ {len(links)} GIS links found")
            else:
                print(f"→ HTTP {page.status}")
        except Exception as e:
            print(f"→ ERROR: {str(e)[:60]}")
        time.sleep(0.3)

    return all_links


def search_arcgis_rest_deep(max_results: int = 500) -> list[dict]:
    """Deep search ArcGIS Online REST API for parcel feature services.
    Goes beyond the initial discovery script with more queries and pagination.
    """
    results = []
    seen_ids = set()

    queries = [
        # Targeted county-level searches per state
        *[f'"{state}" parcels type:"Feature Service"' for state in US_STATES.values()],
        # Generic high-value queries
        'county parcels type:"Feature Service" access:public',
        'assessor property type:"Feature Service" access:public',
        'tax parcels type:"Feature Service" access:public',
        'cadastral type:"Feature Service" access:public',
        'zoning districts type:"Feature Service" access:public',
        'land use type:"Feature Service" access:public',
        'property lines type:"Feature Service" access:public',
        'lot boundaries type:"Feature Service" access:public',
    ]

    base_url = "https://www.arcgis.com/sharing/rest/search"

    for qi, query in enumerate(queries):
        label = query[:50]
        print(f"  [{qi+1}/{len(queries)}] {label}...", end=" ", flush=True)

        start = 1
        query_count = 0

        while query_count < max_results:
            params = {
                "q": query,
                "num": 100,
                "start": start,
                "f": "json",
                "sortField": "numViews",
                "sortOrder": "desc",
            }
            try:
                with httpx.Client(timeout=15) as client:
                    resp = client.get(base_url, params=params)
                    resp.raise_for_status()
                    data = resp.json()

                items = data.get("results", [])
                if not items:
                    break

                for item in items:
                    item_id = item.get("id", "")
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    # US bounding box filter
                    extent = item.get("extent", [])
                    is_us = False
                    if extent and len(extent) == 2:
                        try:
                            lon = (extent[0][0] + extent[1][0]) / 2
                            lat = (extent[0][1] + extent[1][1]) / 2
                            if -130 <= lon <= -60 and 24 <= lat <= 50:
                                is_us = True
                        except (TypeError, IndexError):
                            pass

                    if not is_us:
                        title = (item.get("title") or "").lower()
                        desc = (item.get("description") or "").lower()
                        for state in US_STATES.values():
                            if state.lower() in title or state.lower() in desc:
                                is_us = True
                                break

                    if is_us:
                        results.append({
                            "id": item_id,
                            "title": item.get("title"),
                            "url": item.get("url"),
                            "owner": item.get("owner"),
                            "views": item.get("numViews", 0),
                            "tags": item.get("tags", []),
                            "description": (item.get("snippet") or "")[:200],
                            "extent": extent,
                            "created": item.get("created"),
                            "modified": item.get("modified"),
                        })
                        query_count += 1

                next_start = data.get("nextStart", -1)
                if next_start == -1 or next_start <= start:
                    break
                start = next_start

            except Exception as e:
                print(f"ERR:{str(e)[:30]}", end=" ")
                break

            time.sleep(0.3)

        print(f"→ {query_count} new")

    return results


def verify_endpoint(url: str, timeout: float = 10.0) -> dict:
    """Verify a GIS endpoint is live and serving parcel-like data."""
    result = {"url": url, "alive": False, "type": None, "has_parcels": False}

    if not url:
        return result

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            # ArcGIS REST endpoint
            if any(k in url.lower() for k in ["arcgis", "mapserver", "featureserver"]):
                info_url = url.rstrip("/")
                if not info_url.endswith("?f=json"):
                    info_url += "?f=json"
                resp = client.get(info_url)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if "layers" in data or "name" in data or "services" in data:
                            result["alive"] = True
                            result["type"] = "ArcGIS REST"

                            # Check if any layer mentions parcels
                            layers = data.get("layers", [])
                            services = data.get("services", [])
                            all_names = [l.get("name", "").lower() for l in layers]
                            all_names += [s.get("name", "").lower() for s in services]

                            parcel_keywords = ["parcel", "property", "cadastral",
                                             "assessor", "tax", "lot", "boundary"]
                            result["has_parcels"] = any(
                                kw in name for name in all_names for kw in parcel_keywords
                            )
                            result["layers"] = [l.get("name") for l in layers[:20]]
                            return result
                    except json.JSONDecodeError:
                        pass

            # Generic HTTP check
            resp = client.get(url)
            result["alive"] = resp.status_code == 200
            result["type"] = "HTTP"

            # Check page content for parcel keywords
            if result["alive"]:
                content = resp.text.lower()[:5000]
                result["has_parcels"] = any(
                    kw in content for kw in ["parcel", "property", "cadastral", "assessor"]
                )
    except Exception as e:
        result["error"] = str(e)[:100]

    return result


def deduplicate(new_items: list[dict], existing: dict) -> list[dict]:
    """Remove items that already exist in our index."""
    # Build fingerprint set from existing
    existing_fps = set()
    for entry in existing.values():
        for url_key in ["url", "parcels_url", "api"]:
            u = entry.get(url_key, "")
            if u:
                existing_fps.add(url_fingerprint(u))

    # Also dedup within new items
    seen = set()
    unique = []
    for item in new_items:
        url = item.get("url", "")
        fp = url_fingerprint(url)
        if fp and fp not in existing_fps and fp not in seen:
            seen.add(fp)
            unique.append(item)

    return unique


def merge_and_save(existing: dict, new_verified: list[dict]) -> dict:
    """Merge new verified entries into existing index."""
    for item in new_verified:
        item_id = item.get("id") or url_fingerprint(item.get("url", ""))
        key = f"scraped_{item_id}"
        existing[key] = {
            "source": "scraped",
            "discovered": datetime.now().isoformat(),
            **item,
        }

    with open(COMBINED_JSON, "w") as f:
        json.dump(existing, f, indent=2, default=str)

    return existing


def generate_markdown(results: dict, output_path: str):
    """Regenerate the full markdown index."""
    lines = []
    lines.append("# US Property Parcel Data Sources Index")
    lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"\nTotal sources discovered: **{len(results)}**")

    statewide = [r for r in results.values() if r.get("source") == "statewide"]
    federal = [r for r in results.values() if r.get("source") == "federal"]
    arcgis = [r for r in results.values() if r.get("source") == "arcgis_online"]
    scraped = [r for r in results.values() if r.get("source") == "scraped"]
    verified_count = len([r for r in results.values() if r.get("verified") or r.get("alive")])

    lines.append(f"\n| Category | Count | Verified |")
    lines.append(f"|----------|-------|----------|")
    lines.append(f"| Statewide Parcel Portals | {len(statewide)} | {len([r for r in statewide if r.get('verified')])} |")
    lines.append(f"| Federal Overlay Sources | {len(federal)} | {len([r for r in federal if r.get('verified')])} |")
    lines.append(f"| ArcGIS County/Local Services | {len(arcgis)} | {len([r for r in arcgis if r.get('verified')])} |")
    lines.append(f"| Scraped Discoveries | {len(scraped)} | {len([r for r in scraped if r.get('verified') or r.get('alive')])} |")
    lines.append(f"| **Total** | **{len(results)}** | **{verified_count}** |")

    # Statewide
    lines.append("\n---\n## Statewide Parcel Data (Free)\n")
    for r in sorted(statewide, key=lambda x: x.get("state", "")):
        v = "✅" if r.get("verified") else "❓"
        lines.append(f"### {v} {r.get('state', 'Unknown')}")
        lines.append(f"- **Portal**: {r.get('url', 'N/A')}")
        lines.append(f"- **Parcels**: {r.get('parcels_url', 'N/A')}")
        lines.append(f"- **Format**: {r.get('format', 'N/A')}")
        lines.append(f"- **Notes**: {r.get('notes', '')}\n")

    # Federal
    lines.append("\n---\n## Federal Overlay Data (Free)\n")
    for r in sorted(federal, key=lambda x: x.get("name", "")):
        v = "✅" if r.get("verified") else "❓"
        lines.append(f"### {v} {r.get('name', 'Unknown')}")
        lines.append(f"- **Portal**: {r.get('url', 'N/A')}")
        lines.append(f"- **API**: {r.get('api', 'N/A')}")
        lines.append(f"- **Format**: {r.get('format', 'N/A')}")
        lines.append(f"- **Notes**: {r.get('notes', '')}\n")

    # All ArcGIS + scraped combined, sorted by views
    all_services = arcgis + scraped
    all_services_sorted = sorted(all_services, key=lambda x: x.get("views", 0), reverse=True)

    lines.append(f"\n---\n## Discovered GIS Services ({len(all_services)} total)\n")
    lines.append("| # | Title | Owner | Views | Verified | URL |")
    lines.append("|---|-------|-------|-------|----------|-----|")
    for i, r in enumerate(all_services_sorted[:1000], 1):
        v = "✅" if r.get("verified") or r.get("alive") else "❌"
        title = (r.get("title") or "Untitled")[:55]
        owner = (r.get("owner") or "Unknown")[:25]
        views = r.get("views", 0)
        url = r.get("url") or "N/A"
        if url != "N/A" and len(url) > 70:
            url = url[:67] + "..."
        lines.append(f"| {i} | {title} | {owner} | {views:,} | {v} | {url} |")

    if len(all_services_sorted) > 1000:
        lines.append(f"\n*... and {len(all_services_sorted) - 1000} more (see JSON)*")

    # Missing states
    covered = {r.get("state") for r in statewide if r.get("state")}
    missing = sorted(set(US_STATES.values()) - covered)
    lines.append(f"\n---\n## States Without Statewide Parcel Data ({len(missing)})\n")
    for state in missing:
        lines.append(f"- {state}")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"[✓] Markdown: {output_path}")


def main():
    print("=" * 70)
    print("  GIS Portal Scraper — New Source Discovery")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 70)

    # Load existing
    existing = load_existing()
    print(f"\nExisting index: {len(existing)} entries")

    all_new = []

    # ── Phase 1: Scrape directory pages ──────────────────────────────────
    print(f"\n[Phase 1] Scraping {len(GIS_DIRECTORY_PAGES)} GIS directory pages...")
    try:
        with FetcherSession(impersonate="chrome") as session:
            dir_links = scrape_directory_pages(session)
        print(f"  Raw links found: {len(dir_links)}")
        all_new.extend(dir_links)
    except Exception as e:
        print(f"  [!] Scrapling error: {e}")
        print("  Falling back to httpx...")
        for url in GIS_DIRECTORY_PAGES[:10]:
            try:
                with httpx.Client(timeout=10, follow_redirects=True) as client:
                    resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200:
                        links = extract_gis_links(resp.text, url)
                        all_new.extend(links)
            except Exception:
                pass

    # ── Phase 2: Deep ArcGIS REST search (per-state) ─────────────────────
    print(f"\n[Phase 2] Deep ArcGIS Online search (per-state + keywords)...")
    arcgis_new = search_arcgis_rest_deep(max_results=300)
    print(f"  Total ArcGIS services found: {len(arcgis_new)}")
    all_new.extend(arcgis_new)

    # ── Phase 3: Deduplicate ─────────────────────────────────────────────
    print(f"\n[Phase 3] Deduplicating against {len(existing)} existing entries...")
    unique = deduplicate(all_new, existing)
    print(f"  New unique sources: {len(unique)} (from {len(all_new)} raw)")

    # ── Phase 4: Verify new endpoints ────────────────────────────────────
    print(f"\n[Phase 4] Verifying {len(unique)} new endpoints...")
    verified = []

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {}
        for item in unique:
            url = item.get("url", "")
            if url:
                futures[executor.submit(verify_endpoint, url)] = item

        alive = 0
        dead = 0
        for i, future in enumerate(as_completed(futures)):
            item = futures[future]
            try:
                result = future.result()
                item["verified"] = result["alive"]
                item["alive"] = result["alive"]
                item["endpoint_type"] = result.get("type")
                item["has_parcels"] = result.get("has_parcels", False)
                if result.get("layers"):
                    item["layers"] = result["layers"]

                if result["alive"]:
                    verified.append(item)
                    alive += 1
                else:
                    dead += 1
            except Exception:
                dead += 1

            if (i + 1) % 100 == 0:
                print(f"    Verified {i+1}/{len(futures)}... ({alive} alive, {dead} dead)")

    print(f"  ✓ {len(verified)} verified alive, {dead} dead/unreachable")

    # ── Phase 5: Merge and save ──────────────────────────────────────────
    print(f"\n[Phase 5] Merging {len(verified)} new sources into index...")
    combined = merge_and_save(existing, verified)
    print(f"  Total index size: {len(combined)}")

    # Regenerate markdown
    generate_markdown(combined, str(COMBINED_MD))

    # Save new discoveries separately for review
    with open(NEW_JSON, "w") as f:
        json.dump(verified, f, indent=2, default=str)
    print(f"[✓] New discoveries: {NEW_JSON} ({len(verified)} entries)")

    print(f"\n{'=' * 70}")
    print(f"  DONE — {len(verified)} new sources added, {len(combined)} total")
    print(f"{'=' * 70}")

    return len(verified)


if __name__ == "__main__":
    main()
