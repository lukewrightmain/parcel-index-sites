#!/usr/bin/env python3
"""
County GIS Parcel Data Discovery Script

Discovers and verifies public GIS endpoints for US county parcel data.
Strategy:
1. Start with known statewide parcel portals (free, ~25 states)
2. Query ArcGIS Online/Hub for county parcel services
3. Probe common URL patterns for county GIS sites
4. Verify each endpoint is alive and returns parcel data
5. Output a comprehensive index as markdown + JSON

Requirements: pip install httpx beautifulsoup4 tqdm
"""

import httpx
import json
import time
import re
import sys
import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Known Statewide Parcel Data Sources (Verified Free) ──────────────────────

STATEWIDE_SOURCES = {
    "Arkansas": {
        "url": "https://gis.arkansas.gov",
        "parcels_url": "https://gis.arkansas.gov/product/parcels/",
        "format": "Shapefile/GeoJSON",
        "notes": "Arkansas GIS Office, statewide parcels"
    },
    "Colorado": {
        "url": "https://geodata.co.gov",
        "parcels_url": "https://geodata.co.gov/datasets/parcels",
        "format": "ArcGIS REST/Shapefile",
        "notes": "Colorado Governor's Office of IT"
    },
    "Connecticut": {
        "url": "https://ct-deep-gis-open-data-website-ctdeep.hub.arcgis.com",
        "parcels_url": "https://ct-deep-gis-open-data-website-ctdeep.hub.arcgis.com/datasets/parcels",
        "format": "ArcGIS Hub",
        "notes": "CT DEEP GIS, statewide parcels"
    },
    "Delaware": {
        "url": "https://firstmap.delaware.gov",
        "parcels_url": "https://firstmap.delaware.gov/arcgis/rest/services",
        "format": "ArcGIS REST",
        "notes": "FirstMap Delaware"
    },
    "Idaho": {
        "url": "https://data-idahogeo.hub.arcgis.com",
        "parcels_url": "https://data-idahogeo.hub.arcgis.com/datasets/parcels",
        "format": "ArcGIS Hub",
        "notes": "Idaho Geospatial Office"
    },
    "Indiana": {
        "url": "https://maps.indiana.edu",
        "parcels_url": "https://maps.indiana.edu/layerGallery.html",
        "format": "ArcGIS REST/WMS",
        "notes": "IndianaMap, statewide parcels via county feeds"
    },
    "Iowa": {
        "url": "https://geodata.iowa.gov",
        "parcels_url": "https://geodata.iowa.gov/datasets/parcels",
        "format": "ArcGIS Hub/Shapefile",
        "notes": "Iowa Geodata"
    },
    "Maine": {
        "url": "https://www.maine.gov/geolib",
        "parcels_url": "https://www.maine.gov/geolib/catalog.html",
        "format": "Shapefile",
        "notes": "Maine GeoLibrary, partial statewide"
    },
    "Maryland": {
        "url": "https://data.imap.maryland.gov",
        "parcels_url": "https://data.imap.maryland.gov/datasets/maryland-property-data-parcels",
        "format": "ArcGIS Hub",
        "notes": "Maryland iMap, SDAT property data"
    },
    "Massachusetts": {
        "url": "https://www.mass.gov/info-details/massgis-data-standardized-assessors-parcels",
        "parcels_url": "https://www.mass.gov/info-details/massgis-data-standardized-assessors-parcels",
        "format": "Shapefile/GDB",
        "notes": "MassGIS Level 3 Parcels"
    },
    "Michigan": {
        "url": "https://gis-michigan.opendata.arcgis.com",
        "parcels_url": "https://gis-michigan.opendata.arcgis.com/datasets/parcels",
        "format": "ArcGIS Hub",
        "notes": "State of Michigan GIS, statewide parcels"
    },
    "Minnesota": {
        "url": "https://gisdata.mn.gov",
        "parcels_url": "https://gisdata.mn.gov/dataset/plan-regional-parcels",
        "format": "Shapefile/GDB",
        "notes": "MnGeo, statewide county parcels"
    },
    "Mississippi": {
        "url": "https://www.maris.state.ms.us",
        "parcels_url": "https://www.maris.state.ms.us/HTM/DownloadData/Parcels.html",
        "format": "Shapefile",
        "notes": "MARIS, statewide parcels"
    },
    "Montana": {
        "url": "https://svc.mt.gov/msl/mtcadastral",
        "parcels_url": "https://svc.mt.gov/msl/mtcadastral",
        "format": "ArcGIS REST/WMS",
        "notes": "Montana Cadastral, statewide ownership parcels"
    },
    "Nebraska": {
        "url": "https://www.nebraskamap.gov",
        "parcels_url": "https://www.nebraskamap.gov/datasets/parcels",
        "format": "ArcGIS Hub",
        "notes": "NebraskaMap"
    },
    "New Jersey": {
        "url": "https://njgin.nj.gov",
        "parcels_url": "https://njgin.nj.gov/njgin/edata/parcels/",
        "format": "ArcGIS REST/GDB",
        "notes": "NJ Geographic Information Network, MOD-IV tax parcels"
    },
    "New Mexico": {
        "url": "https://rgis.unm.edu",
        "parcels_url": "https://rgis.unm.edu/rgis6/",
        "format": "Shapefile",
        "notes": "RGIS (Resource Geographic Info System)"
    },
    "North Carolina": {
        "url": "https://www.nconemap.gov",
        "parcels_url": "https://www.nconemap.gov/datasets/parcels",
        "format": "ArcGIS Hub/REST",
        "notes": "NC OneMap, statewide parcels"
    },
    "Oregon": {
        "url": "https://geo.oregon.gov",
        "parcels_url": "https://geo.oregon.gov/imagery/rest/services/Parcels",
        "format": "ArcGIS REST",
        "notes": "Oregon Spatial Data Library"
    },
    "Pennsylvania": {
        "url": "https://newdata-dcnr.opendata.arcgis.com",
        "parcels_url": "https://www.pasda.psu.edu",
        "format": "Shapefile/Various",
        "notes": "PASDA (PA Spatial Data Access), county-by-county"
    },
    "South Carolina": {
        "url": "https://www.sccounties.org/gis",
        "parcels_url": "https://sc-department-of-revenue.hub.arcgis.com",
        "format": "ArcGIS Hub",
        "notes": "SC Revenue parcel data"
    },
    "Utah": {
        "url": "https://gis.utah.gov",
        "parcels_url": "https://opendata.gis.utah.gov/datasets/utah-parcels",
        "format": "ArcGIS Hub/Shapefile",
        "notes": "UGRC (Utah Geospatial Resource Center)"
    },
    "Vermont": {
        "url": "https://geodata.vermont.gov",
        "parcels_url": "https://geodata.vermont.gov/datasets/vt-data-statewide-standardized-parcel-data",
        "format": "ArcGIS Hub",
        "notes": "Vermont Center for Geographic Information"
    },
    "Virginia": {
        "url": "https://vgin.vdem.virginia.gov",
        "parcels_url": "https://vgin.vdem.virginia.gov/datasets/parcels",
        "format": "ArcGIS REST",
        "notes": "VGIN, statewide parcels (partial county coverage)"
    },
    "Wisconsin": {
        "url": "https://data-ltsb.opendata.arcgis.com",
        "parcels_url": "https://data-ltsb.opendata.arcgis.com/datasets/wi-parcels",
        "format": "ArcGIS Hub/V3 Parcels",
        "notes": "WI Legislative Technology Services Bureau, statewide V3 parcels"
    },
    "Wyoming": {
        "url": "https://wyogeo.wygisc.org",
        "parcels_url": "https://wyogeo.wygisc.org/arcgis/rest/services",
        "format": "ArcGIS REST",
        "notes": "WyoGeo, statewide parcels"
    },
}

# ── Federal / Free Overlay Data Sources ──────────────────────────────────────

FEDERAL_OVERLAY_SOURCES = {
    "National Wetlands Inventory (NWI)": {
        "url": "https://fwsprimary.wim.usgs.gov/wetlands/apps/wetlands-mapper/",
        "api": "https://www.fws.gov/wetlands/Data/State-Downloads.html",
        "format": "Shapefile/GDB/WMS",
        "notes": "US Fish & Wildlife, wetland boundaries & classification"
    },
    "NLCD Land Cover": {
        "url": "https://www.mrlc.gov/data",
        "api": "https://www.mrlc.gov/data/nlcd-land-cover-conus-all-years",
        "format": "GeoTIFF raster (30m resolution)",
        "notes": "Forest, urban, water, cropland, etc. Updated every 2-3 years"
    },
    "USDA Cropland Data Layer": {
        "url": "https://nassgeodata.gmu.edu/CropScape/",
        "api": "https://nassgeodata.gmu.edu/CropScape/devhelp/getexamples.html",
        "format": "GeoTIFF/WMS",
        "notes": "Crop-specific land cover, annual updates"
    },
    "FEMA Flood Zones (NFHL)": {
        "url": "https://msc.fema.gov/portal/home",
        "api": "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer",
        "format": "ArcGIS REST/Shapefile",
        "notes": "Flood hazard zones, updated per community"
    },
    "PAD-US Protected Areas": {
        "url": "https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-overview",
        "api": "https://maps.usgs.gov/padus/rest/services",
        "format": "GDB/Shapefile/ArcGIS REST",
        "notes": "Federal, state, local, private protected lands"
    },
    "Census TIGER/Line (Boundaries)": {
        "url": "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
        "api": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb",
        "format": "Shapefile/ArcGIS REST",
        "notes": "County, tract, block group boundaries — NOT individual parcels"
    },
    "BLM Public Land Survey (PLSS)": {
        "url": "https://navigator.blm.gov",
        "api": "https://gis.blm.gov/arcgis/rest/services/Cadastral",
        "format": "ArcGIS REST/Shapefile",
        "notes": "Township, range, section grid for western states"
    },
    "USGS National Map": {
        "url": "https://apps.nationalmap.gov",
        "api": "https://services.nationalmap.gov/arcgis/rest/services",
        "format": "ArcGIS REST/WMS/Various",
        "notes": "Elevation, hydro, boundaries, land cover — base layers"
    },
}

# ── ArcGIS Online Search (Discovers thousands of county endpoints) ───────────

ARCGIS_SEARCH_QUERIES = [
    "parcels",
    "tax parcels",
    "property parcels",
    "cadastral parcels",
    "assessor parcels",
    "county parcels",
    "parcel boundaries",
    "property boundaries",
    "land parcels",
    "parcel data",
    "zoning",
    "land use",
]

# All 50 US states + DC for FIPS lookup
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


def search_arcgis_hub(query: str, max_results: int = 100, timeout: float = 15.0) -> list[dict]:
    """Search ArcGIS Online for public parcel feature services."""
    results = []
    start = 1
    base_url = "https://www.arcgis.com/sharing/rest/search"

    while len(results) < max_results:
        params = {
            "q": f'{query} type:"Feature Service" access:public',
            "num": min(100, max_results - len(results)),
            "start": start,
            "f": "json",
            "sortField": "numViews",
            "sortOrder": "desc",
        }
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(base_url, params=params)
                resp.raise_for_status()
                data = resp.json()

            for item in data.get("results", []):
                # Filter to US-related content
                extent = item.get("extent", [])
                tags = " ".join(item.get("tags", [])).lower()
                title = item.get("title", "").lower()
                desc = (item.get("description") or "").lower()

                # Basic US filter: check extent or keywords
                is_us = False
                if extent and len(extent) == 2:
                    # US rough bounding box: lon -130 to -60, lat 24 to 50
                    try:
                        lon = (extent[0][0] + extent[1][0]) / 2
                        lat = (extent[0][1] + extent[1][1]) / 2
                        if -130 <= lon <= -60 and 24 <= lat <= 50:
                            is_us = True
                    except (TypeError, IndexError):
                        pass

                if not is_us:
                    for state in US_STATES.values():
                        if state.lower() in title or state.lower() in desc:
                            is_us = True
                            break

                if not is_us:
                    for abbr in US_STATES.keys():
                        if f" {abbr.lower()} " in f" {title} " or f" {abbr.lower()} " in f" {tags} ":
                            is_us = True
                            break

                if is_us:
                    results.append({
                        "title": item.get("title"),
                        "url": item.get("url"),
                        "owner": item.get("owner"),
                        "type": item.get("type"),
                        "views": item.get("numViews", 0),
                        "created": item.get("created"),
                        "modified": item.get("modified"),
                        "tags": item.get("tags", []),
                        "extent": extent,
                        "description": (item.get("snippet") or "")[:200],
                        "access": item.get("access"),
                        "id": item.get("id"),
                    })

            next_start = data.get("nextStart", -1)
            if next_start == -1 or next_start <= start:
                break
            start = next_start

        except Exception as e:
            print(f"  [!] ArcGIS search error for '{query}': {e}", file=sys.stderr)
            break

    return results


def verify_endpoint(url: str, timeout: float = 10.0) -> dict:
    """Verify a GIS endpoint is live and returns data."""
    result = {"url": url, "alive": False, "type": None, "record_count": None}

    if not url:
        return result

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            # Try ArcGIS REST info query
            if "arcgis" in url.lower() or "mapserver" in url.lower() or "featureserver" in url.lower():
                info_url = url.rstrip("/") + "?f=json"
                resp = client.get(info_url)
                if resp.status_code == 200:
                    data = resp.json()
                    if "layers" in data or "name" in data:
                        result["alive"] = True
                        result["type"] = "ArcGIS REST"
                        layers = data.get("layers", [])
                        result["layers"] = len(layers)
                        result["layer_names"] = [l.get("name", "") for l in layers[:10]]
                        return result

            # Try plain HTTP
            resp = client.get(url)
            result["alive"] = resp.status_code == 200
            result["type"] = "HTTP"
            result["status_code"] = resp.status_code

    except Exception as e:
        result["error"] = str(e)[:100]

    return result


def discover_all(max_per_query: int = 200, verify: bool = True, parallel: int = 10):
    """Run full discovery pipeline."""
    print("=" * 70)
    print("  US County GIS Parcel Data Discovery")
    print(f"  Started: {datetime.now().isoformat()}")
    print("=" * 70)

    all_results = {}

    # ── Phase 1: Known statewide sources ─────────────────────────────────
    print(f"\n[Phase 1] {len(STATEWIDE_SOURCES)} known statewide parcel sources")
    for state, info in STATEWIDE_SOURCES.items():
        all_results[f"state_{state}"] = {
            "source": "statewide",
            "state": state,
            **info,
        }
    print(f"  ✓ {len(STATEWIDE_SOURCES)} statewide sources indexed")

    # ── Phase 2: Federal overlay sources ─────────────────────────────────
    print(f"\n[Phase 2] {len(FEDERAL_OVERLAY_SOURCES)} federal overlay sources")
    for name, info in FEDERAL_OVERLAY_SOURCES.items():
        all_results[f"federal_{name}"] = {
            "source": "federal",
            "name": name,
            **info,
        }
    print(f"  ✓ {len(FEDERAL_OVERLAY_SOURCES)} federal sources indexed")

    # ── Phase 3: ArcGIS Online search ────────────────────────────────────
    print(f"\n[Phase 3] Searching ArcGIS Online ({len(ARCGIS_SEARCH_QUERIES)} queries, up to {max_per_query} results each)")
    arcgis_results = {}
    seen_ids = set()

    for i, query in enumerate(ARCGIS_SEARCH_QUERIES):
        print(f"  [{i+1}/{len(ARCGIS_SEARCH_QUERIES)}] Searching: '{query}'...", end=" ", flush=True)
        results = search_arcgis_hub(query, max_results=max_per_query)
        new_count = 0
        for r in results:
            rid = r.get("id", r.get("url", ""))
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                key = f"arcgis_{rid}"
                arcgis_results[key] = {
                    "source": "arcgis_online",
                    **r,
                }
                new_count += 1
        print(f"{len(results)} found, {new_count} new")
        time.sleep(0.5)  # Rate limit

    all_results.update(arcgis_results)
    print(f"  ✓ {len(arcgis_results)} unique ArcGIS services discovered")

    # ── Phase 4: Verify endpoints ────────────────────────────────────────
    if verify:
        urls_to_verify = []
        for key, info in all_results.items():
            url = info.get("parcels_url") or info.get("url") or info.get("api")
            if url:
                urls_to_verify.append((key, url))

        print(f"\n[Phase 4] Verifying {len(urls_to_verify)} endpoints ({parallel} threads)...")
        verified = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {}
            for key, url in urls_to_verify:
                futures[executor.submit(verify_endpoint, url)] = key

            for i, future in enumerate(as_completed(futures)):
                key = futures[future]
                try:
                    result = future.result()
                    all_results[key]["verified"] = result["alive"]
                    all_results[key]["endpoint_type"] = result.get("type")
                    if result["alive"]:
                        verified += 1
                    else:
                        failed += 1
                except Exception:
                    all_results[key]["verified"] = False
                    failed += 1

                if (i + 1) % 50 == 0:
                    print(f"    Verified {i+1}/{len(urls_to_verify)}... ({verified} alive, {failed} failed)")

        print(f"  ✓ {verified} alive, {failed} failed/unreachable")

    # ── Output ───────────────────────────────────────────────────────────
    return all_results


def generate_markdown(results: dict, output_path: str):
    """Generate markdown index from results."""
    lines = []
    lines.append("# US Property Parcel Data Sources Index")
    lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"\nTotal sources discovered: **{len(results)}**")

    # Summary counts
    statewide = [r for r in results.values() if r.get("source") == "statewide"]
    federal = [r for r in results.values() if r.get("source") == "federal"]
    arcgis = [r for r in results.values() if r.get("source") == "arcgis_online"]
    verified_count = len([r for r in results.values() if r.get("verified")])

    lines.append(f"\n| Category | Count | Verified |")
    lines.append(f"|----------|-------|----------|")
    lines.append(f"| Statewide Parcel Portals | {len(statewide)} | {len([r for r in statewide if r.get('verified')])} |")
    lines.append(f"| Federal Overlay Sources | {len(federal)} | {len([r for r in federal if r.get('verified')])} |")
    lines.append(f"| ArcGIS County/Local Services | {len(arcgis)} | {len([r for r in arcgis if r.get('verified')])} |")
    lines.append(f"| **Total** | **{len(results)}** | **{verified_count}** |")

    # ── Statewide sources ────────────────────────────────────────────────
    lines.append("\n---\n## Statewide Parcel Data (Free)")
    lines.append("\nThese states provide aggregated statewide parcel datasets at no cost.\n")
    for r in sorted(statewide, key=lambda x: x.get("state", "")):
        v = "✅" if r.get("verified") else "❓"
        lines.append(f"### {v} {r['state']}")
        lines.append(f"- **Portal**: {r.get('url', 'N/A')}")
        lines.append(f"- **Parcels**: {r.get('parcels_url', 'N/A')}")
        lines.append(f"- **Format**: {r.get('format', 'N/A')}")
        lines.append(f"- **Notes**: {r.get('notes', '')}")
        lines.append("")

    # ── Federal sources ──────────────────────────────────────────────────
    lines.append("\n---\n## Federal Overlay Data (Free)")
    lines.append("\nLand classification, wetlands, flood zones, etc.\n")
    for r in sorted(federal, key=lambda x: x.get("name", "")):
        v = "✅" if r.get("verified") else "❓"
        lines.append(f"### {v} {r['name']}")
        lines.append(f"- **Portal**: {r.get('url', 'N/A')}")
        lines.append(f"- **API**: {r.get('api', 'N/A')}")
        lines.append(f"- **Format**: {r.get('format', 'N/A')}")
        lines.append(f"- **Notes**: {r.get('notes', '')}")
        lines.append("")

    # ── ArcGIS discovered (group by state if possible) ───────────────────
    lines.append("\n---\n## ArcGIS Online Discovered Services")
    lines.append(f"\nFound via ArcGIS Online search API. {len(arcgis)} services.\n")

    # Sort by views (most popular = most used = likely best)
    arcgis_sorted = sorted(arcgis, key=lambda x: x.get("views", 0), reverse=True)

    lines.append("| # | Title | Owner | Views | Verified | URL |")
    lines.append("|---|-------|-------|-------|----------|-----|")
    for i, r in enumerate(arcgis_sorted[:500], 1):  # Cap at 500 for readability
        v = "✅" if r.get("verified") else "❌" if r.get("verified") is False else "❓"
        title = (r.get("title") or "Untitled")[:60]
        owner = (r.get("owner") or "Unknown")[:30]
        views = r.get("views", 0)
        url = r.get("url") or "N/A"
        if url != "N/A" and len(url) > 80:
            url = url[:77] + "..."
        lines.append(f"| {i} | {title} | {owner} | {views:,} | {v} | {url} |")

    if len(arcgis_sorted) > 500:
        lines.append(f"\n*... and {len(arcgis_sorted) - 500} more (see JSON output for full list)*")

    # ── States with NO statewide data ────────────────────────────────────
    covered = {r.get("state") for r in statewide}
    missing = sorted(set(US_STATES.values()) - covered)
    lines.append(f"\n---\n## States Without Statewide Parcel Data ({len(missing)})")
    lines.append("\nThese require county-by-county scraping:\n")
    for state in missing:
        lines.append(f"- {state}")

    lines.append("\n---\n## Next Steps")
    lines.append("""
1. **Prioritize the 26 free statewide sources** — download/connect these first
2. **Use the ArcGIS discovered services** — many are individual county parcel layers
3. **For missing states**, search each county's assessor/GIS website:
   - Google: `"[county name] county" GIS parcels site:.gov`
   - Check if they use ArcGIS Online (most do)
   - Some counties only offer data via FOIA request or in-person
4. **Overlay federal data** (wetlands, land cover, flood zones) on top of parcels
5. **Commercial fallback**: Regrid API for counties you can't find free data for
""")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\n[✓] Markdown written to {output_path}")


def main():
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    json_path = output_dir / "parcel_sources.json"
    md_path = output_dir / "PARCEL_DATA_SOURCES.md"

    print("\nThis script will:")
    print("  1. Index 26 known statewide parcel portals")
    print("  2. Index 8 federal overlay data sources")
    print("  3. Search ArcGIS Online for county-level parcel services")
    print("  4. Verify all endpoints are alive")
    print(f"\nOutput: {md_path}")
    print(f"        {json_path}\n")

    results = discover_all(
        max_per_query=200,
        verify=True,
        parallel=15,
    )

    # Save JSON
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"[✓] JSON written to {json_path} ({len(results)} entries)")

    # Save Markdown
    generate_markdown(results, str(md_path))

    print(f"\n{'=' * 70}")
    print(f"  DONE — {len(results)} total sources indexed")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
