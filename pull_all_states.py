#!/usr/bin/env python3
"""
Pull parcel data from ALL states in the index.
Outputs one GeoJSON file per source, organized by state.
Compresses into per-state .tar.gz for GitHub.
"""

import httpx
import json
import sys
import os
import re
import time
import tarfile
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR = Path(__file__).parent.parent / "data"
SOURCES_JSON = DATA_DIR / "parcel_sources.json"
PARCELS_DIR = DATA_DIR / "parcels_by_state"
ARCHIVE_DIR = DATA_DIR / "datasets"

PARCELS_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

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

# Field mappings (same as pull_parcel_data.py)
FIELD_MAP = {
    "parcel_id": ["APN", "PIN", "PID", "PARCEL_ID", "PARCELID", "PARCELNB", "PARCELNO",
                  "TAX_ID", "ACCOUNT_NO", "ACCTID", "PARCEL", "PARCEL_NUM", "PARCELNUM",
                  "TAXPIN", "OBJECTID", "GIS_PIN", "MAPNO", "PARCEL_NUMBER", "PARID",
                  "PARCELKEY", "PROP_ID", "PROPERTYID", "ACCOUNT", "ACCOUNTNO", "ACCT",
                  "TAXID", "TAXKEY", "FOLIO", "ROLL_NUM", "ROLLNUM"],
    "owner_name": ["OWNER", "OWNER1", "OWNERNAME", "OWNER_NAME", "GRANTEE", "TAXPAYER",
                   "OWNERNME", "OWN_NAME", "OWNNAME", "OWNERNAM", "PROPERTY_OWNER",
                   "OWNER_1", "OWNR_NAME", "GRANTEENM", "LAST_NAME", "OWNERFULL",
                   "OWNERLINE1", "NAME", "OWNRNAME"],
    "owner_address": ["MAIL_ADDR", "MAILADDR", "MAILING_ADDRESS", "OWNER_ADDR",
                      "OWNERADDR", "MAIL_ADD", "MAILADDRESS", "MAILADD1", "MAIL_LINE1"],
    "address": ["SITEADDR", "SITE_ADDR", "PROP_ADDR", "ADDRESS", "LOCATION", "SITUS",
                "SITEADDRESS", "SITUSADDR", "SITE_ADDRESS", "PROPADDR", "LOC_ADDR",
                "PROPERTY_ADDRESS", "PHYADDR", "PHYSADDR", "PHYSICAL_ADDRESS",
                "STREETADDR", "SITUS_ADDR", "FULLADDR", "PROPADR"],
    "city": ["CITY", "SITUSCITY", "SITUS_CITY", "PROP_CITY", "SITE_CITY", "MUNI",
             "MUNICIPALITY", "TOWN"],
    "zip": ["ZIP", "ZIPCODE", "ZIP_CODE", "SITUS_ZIP", "PROP_ZIP", "POSTALCODE"],
    "acreage": ["ACRES", "ACREAGE", "AREA_ACRES", "CALCACRES", "GIS_ACRES", "LANDAREA",
                "TOTALACRES", "TOTAL_ACRES", "LOT_ACRES", "DEED_ACRES", "DEEDACRES",
                "GISACRES", "PARCELACRE", "SQFT_ACRES"],
    "sq_ft": ["SQFT", "SQ_FT", "BLDG_SQFT", "TOTSQFT", "GROSSAREA", "LIVEAREA",
              "LOTSQFT", "LOT_SIZE", "LOTSIZE", "LANDSQFT", "SHAPE_AREA",
              "PARCELAREA", "TOTAL_SQFT"],
    "land_use_code": ["LANDUSE", "LAND_USE", "USE_CODE", "PROP_CLASS", "CLASS_CODE",
                      "LUC", "USECODE", "USE_TYPE", "PROPCLASS", "CLASSCD",
                      "LAND_USE_CODE", "LUCODE", "PROPERTY_CLASS", "CLASSCODE"],
    "land_use": ["LANDUSE_DESC", "USE_DESC", "USEDESC", "LAND_USE_DESC",
                 "PROP_CLASS_DESC", "CLASS_DESC", "USE_DESCRIPTION", "PROPTYPE",
                 "PROPERTY_TYPE", "LANDUSEDESC"],
    "zoning_code": ["ZONING", "ZONE", "ZONE_CODE", "ZONING_CODE", "ZONEDIST",
                    "ZONECLASS", "ZONE_TYPE", "ZONINGCODE", "ZN_CODE"],
    "zoning": ["ZONEDESC", "ZONE_DESC", "ZONING_DESC", "ZONING_DESCRIPTION",
               "ZONE_DESCRIPTION", "ZONINGDESC"],
    "assessed_value": ["ASSESSED", "ASSD_VAL", "TOTAL_AV", "ASSESSED_VALUE",
                       "TOTALVAL", "TOTAL_VAL", "ASSESSEDVALUE", "TOTALASSD",
                       "ASSDTOTAL", "TOT_ASSD", "TOTAL_ASSESSED", "TOTASSESS"],
    "land_value": ["LANDVAL", "LAND_VAL", "LAND_VALUE", "LANDASSD", "LANDASSESS",
                   "LAND_ASSESSED", "LAND_AV"],
    "improvement_value": ["IMPVAL", "IMP_VAL", "IMPROVEMENT_VALUE", "IMPRVAL",
                         "BLDGVAL", "BLDG_VAL", "BUILDING_VALUE", "IMPRASSD"],
    "market_value": ["MARKET_VAL", "MKT_VAL", "MARKET_VALUE", "MKTVAL",
                     "FAIR_MARKET", "FMV", "APPRAISED", "APPRAISEDVALUE"],
    "year_built": ["YEARBUILT", "YR_BUILT", "YEAR_BUILT", "YRBUILT", "EFFYRBUILT",
                   "YRBLT", "BUILT_YEAR", "YEARCONST"],
    "building_sq_ft": ["BLDGSQFT", "BLDG_SQFT", "BUILDING_SQFT", "HEATEDAREA",
                       "HEATED_SQFT", "LIVINGSQFT", "LIVING_AREA", "BLDG_AREA"],
}


def normalize_fields(raw_props: dict) -> dict:
    raw_upper = {k.upper(): v for k, v in raw_props.items()}
    normalized = {}
    for our_field, source_names in FIELD_MAP.items():
        for src in source_names:
            if src in raw_upper and raw_upper[src] is not None:
                val = raw_upper[src]
                if isinstance(val, str):
                    val = val.strip()
                    if val in ("", "NULL", "None", "N/A", "UNKNOWN"):
                        continue
                normalized[our_field] = val
                break
    return normalized


def classify_land_use(code: str, desc: str) -> str:
    combined = f"{code} {desc}".upper()
    if any(k in combined for k in ["RESID", "SINGLE FAM", "MULTI FAM", "DWELLING",
                                     "HOME", "CONDO", "APARTMENT", "TOWNHOUSE", "SFR"]):
        return "residential"
    if any(k in combined for k in ["COMMER", "RETAIL", "OFFICE", "STORE", "SHOP"]):
        return "commercial"
    if any(k in combined for k in ["INDUST", "MANUFACT", "WAREHOUSE", "FACTORY"]):
        return "industrial"
    if any(k in combined for k in ["AGRIC", "FARM", "RANCH", "CROP", "TIMBER",
                                     "PASTURE", "ORCHARD", "GRAZING"]):
        return "agricultural"
    if any(k in combined for k in ["VACANT", "EMPTY", "UNDEVELOPED", "RAW LAND"]):
        return "vacant"
    if any(k in combined for k in ["GOVERN", "PUBLIC", "FEDERAL", "STATE", "COUNTY",
                                     "MUNICIPAL", "SCHOOL", "CHURCH", "EXEMPT"]):
        return "government"
    return "other"


def find_parcel_layer(base_url: str) -> tuple[int, str]:
    try:
        with httpx.Client(timeout=12, follow_redirects=True) as client:
            resp = client.get(f"{base_url.rstrip('/')}?f=json")
            resp.raise_for_status()
            data = resp.json()
        layers = data.get("layers", [])
        if not layers:
            return (0, "default")
        for layer in layers:
            if "parcel" in (layer.get("name") or "").lower():
                return (layer["id"], layer["name"])
        for layer in layers:
            name = (layer.get("name") or "").lower()
            if any(kw in name for kw in ["property", "cadastral", "tax", "lot", "assessor", "boundary"]):
                return (layer["id"], layer["name"])
        return (layers[0]["id"], layers[0].get("name", "Layer 0"))
    except Exception:
        return (0, "default")


def pull_source(src: dict, max_features: int = 100) -> dict:
    """Pull parcel data from one ArcGIS source."""
    url = src.get("url", "")
    title = src.get("title", "Unknown")
    result = {"url": url, "title": title, "success": False, "features": 0, "fields": []}

    if not url or not any(k in url.lower() for k in ["arcgis", "mapserver", "featureserver", "rest/services"]):
        result["reason"] = "not_arcgis"
        return result

    layer_id, layer_name = find_parcel_layer(url)
    query_url = f"{url.rstrip('/')}/{layer_id}/query"

    try:
        with httpx.Client(timeout=25, follow_redirects=True) as client:
            resp = client.get(query_url, params={
                "where": "1=1", "outFields": "*", "f": "geojson",
                "resultRecordCount": max_features, "returnGeometry": "true", "outSR": "4326",
            })
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        result["reason"] = f"query_failed: {str(e)[:50]}"
        return result

    features = data.get("features", [])
    if not features:
        result["reason"] = "no_features"
        return result

    # Normalize
    norm_features = []
    for feat in features:
        raw = feat.get("properties", {})
        norm = normalize_fields(raw)
        norm["land_use_normalized"] = classify_land_use(
            str(norm.get("land_use_code", "")), str(norm.get("land_use", "")))
        norm["source_url"] = url
        norm_features.append({
            "type": "Feature",
            "geometry": feat.get("geometry"),
            "properties": norm,
        })

    result["success"] = True
    result["features"] = len(norm_features)
    result["fields"] = list(norm_features[0]["properties"].keys()) if norm_features else []
    result["layer"] = layer_name
    result["geojson"] = {
        "type": "FeatureCollection",
        "metadata": {
            "source_url": url, "title": title, "layer": layer_name,
            "count": len(norm_features), "pulled_at": datetime.now().isoformat(),
        },
        "features": norm_features,
    }

    # Check data quality
    has_owner = any(f.get("properties", {}).get("owner_name") for f in norm_features)
    has_address = any(f.get("properties", {}).get("address") for f in norm_features)
    has_acreage = any(f.get("properties", {}).get("acreage") for f in norm_features)
    result["has_owner"] = has_owner
    result["has_address"] = has_address
    result["has_acreage"] = has_acreage
    result["quality_score"] = sum([has_owner, has_address, has_acreage])

    return result


def identify_state(src: dict) -> str:
    """Try to identify which state a source belongs to."""
    title = (src.get("title") or "").lower()
    tags = " ".join(src.get("tags", [])).lower()
    desc = (src.get("description") or "").lower()
    owner = (src.get("owner") or "").lower()
    combined = f"{title} {tags} {desc} {owner}"

    for abbr, state in US_STATES.items():
        sl = state.lower()
        if sl in combined:
            return state
        # Check abbreviation in owner or title more carefully
        if f"_{abbr.lower()}" in combined or f" {abbr.lower()} " in f" {combined} ":
            return state
    return "Unknown"


def main():
    print("=" * 70)
    print("  Full US Parcel Data Pull — All States")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 70)

    with open(SOURCES_JSON) as f:
        sources = json.load(f)

    # Filter to pullable parcel services
    pullable = []
    for key, src in sources.items():
        url = src.get("url", "")
        if not url or not (src.get("verified") or src.get("alive")):
            continue
        if not any(k in url.lower() for k in ["arcgis", "mapserver", "featureserver"]):
            continue
        title = (src.get("title") or "").lower()
        tags = " ".join(src.get("tags", [])).lower()
        if any(kw in f"{title} {tags}" for kw in ["parcel", "property", "cadastral", "tax",
                                                     "assessor", "lot", "land", "zoning"]):
            src["_state"] = identify_state(src)
            pullable.append(src)

    # Sort by views
    pullable.sort(key=lambda x: x.get("views", 0), reverse=True)

    print(f"\nTotal pullable sources: {len(pullable)}")

    # Group by state
    by_state = {}
    for src in pullable:
        state = src["_state"]
        by_state.setdefault(state, []).append(src)

    print(f"States represented: {len(by_state)}")
    for state in sorted(by_state.keys()):
        print(f"  {state:25s}: {len(by_state[state]):4d} sources")

    # Pull all
    MAX_FEATURES_PER_SOURCE = 100
    PARALLEL = 20
    total_success = 0
    total_features = 0
    total_attempted = 0
    state_stats = {}

    for state in sorted(by_state.keys()):
        state_sources = by_state[state]
        state_dir = PARCELS_DIR / state.replace(" ", "_")
        state_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'─' * 50}")
        print(f"  {state} ({len(state_sources)} sources)")
        print(f"{'─' * 50}")

        state_success = 0
        state_features = 0
        state_results = []

        with ThreadPoolExecutor(max_workers=PARALLEL) as executor:
            futures = {
                executor.submit(pull_source, src, MAX_FEATURES_PER_SOURCE): src
                for src in state_sources
            }

            for i, future in enumerate(as_completed(futures)):
                total_attempted += 1
                try:
                    result = future.result()
                    if result["success"]:
                        state_success += 1
                        total_success += 1
                        state_features += result["features"]
                        total_features += result["features"]

                        # Save GeoJSON
                        safe_name = re.sub(r'[^\w\-]', '_', result["title"].lower())[:60]
                        out_path = state_dir / f"{safe_name}.geojson"
                        with open(out_path, "w") as f:
                            json.dump(result["geojson"], f)

                        q = result["quality_score"]
                        qmark = "★" * q + "☆" * (3 - q)
                        result_entry = {
                            "title": result["title"],
                            "url": result["url"],
                            "features": result["features"],
                            "has_owner": result["has_owner"],
                            "has_address": result["has_address"],
                            "has_acreage": result["has_acreage"],
                            "file": str(out_path.name),
                        }
                        state_results.append(result_entry)

                        if (i + 1) % 20 == 0 or result["quality_score"] >= 2:
                            print(f"  ✅ {qmark} {result['title'][:45]} — {result['features']} feat")
                    else:
                        if (i + 1) % 50 == 0:
                            print(f"  ... {i+1}/{len(state_sources)} processed ({state_success} success)")
                except Exception as e:
                    pass

        # Save state manifest
        manifest = {
            "state": state,
            "sources_attempted": len(state_sources),
            "sources_successful": state_success,
            "total_features": state_features,
            "pulled_at": datetime.now().isoformat(),
            "datasets": state_results,
        }
        with open(state_dir / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)

        # Compress state directory
        archive_name = f"{state.replace(' ', '_').lower()}_parcels.tar.gz"
        archive_path = ARCHIVE_DIR / archive_name
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(state_dir, arcname=state.replace(" ", "_"))

        size_mb = archive_path.stat().st_size / 1024 / 1024
        state_stats[state] = {
            "sources": len(state_sources),
            "success": state_success,
            "features": state_features,
            "archive": archive_name,
            "size_mb": round(size_mb, 2),
        }

        print(f"  → {state}: {state_success}/{len(state_sources)} sources, {state_features:,} features, {size_mb:.1f}MB archive")

    # Final summary
    print(f"\n{'=' * 70}")
    print(f"  COMPLETE")
    print(f"  Sources attempted:  {total_attempted}")
    print(f"  Successful:         {total_success}")
    print(f"  Total features:     {total_features:,}")
    print(f"  State archives:     {ARCHIVE_DIR}")
    print(f"{'=' * 70}")

    # Save overall report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_attempted": total_attempted,
        "total_successful": total_success,
        "total_features": total_features,
        "states": state_stats,
    }
    with open(DATA_DIR / "full_pull_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport: {DATA_DIR / 'full_pull_report.json'}")


if __name__ == "__main__":
    main()
