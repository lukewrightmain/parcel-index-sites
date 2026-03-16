# US Parcel Data Index & Datasets

Open-source index of **14,660+ verified GIS endpoints** for US property parcel data, plus tools to pull, normalize, and use the data.

Covers all 50 states — property lines, owner names, acreage, zoning, land use, assessed values, and more. All from public county/state/federal sources.

## What's in this repo

```
├── README.md                          # This file
├── SCHEMA.md                          # Normalized data schema
├── parcel_sources.json                # Master index (14,660 GIS endpoints)
├── datasets/                          # Per-state compressed parcel data
│   ├── alabama_parcels.tar.gz
│   ├── alaska_parcels.tar.gz
│   ├── ...
│   └── wyoming_parcels.tar.gz
├── discover_county_gis.py             # Discovery script (finds new GIS endpoints)
├── scrape_gis_portals.py              # Deep scraper (Scrapling + ArcGIS search)
└── pull_all_states.py                 # Data puller (queries all endpoints)
```

## Quick Start

### 1. Browse the source index

`parcel_sources.json` contains 14,660 verified GIS endpoints. Each entry looks like:

```json
{
  "source": "arcgis_online",
  "title": "Parcels_open_data",
  "url": "https://services.arcgis.com/.../FeatureServer",
  "owner": "WeldCounty",
  "views": 20792126,
  "verified": true,
  "tags": ["parcels", "property", "tax"],
  "description": "Weld County parcel boundaries..."
}
```

**Filter by keyword:**
```bash
# Find all parcel services in Texas
python3 -c "
import json
sources = json.load(open('parcel_sources.json'))
for k, v in sources.items():
    title = (v.get('title') or '').lower()
    tags = ' '.join(v.get('tags', [])).lower()
    if 'texas' in f'{title} {tags}' and 'parcel' in f'{title} {tags}':
        print(f\"{v.get('views',0):>10,} views | {v['title'][:50]} | {v.get('url','')[:60]}\")
"
```

### 2. Use the datasets

Each state archive contains GeoJSON files — one per data source (typically one per county or city).

```bash
# Extract a state
tar xzf datasets/north_carolina_parcels.tar.gz

# List what's inside
ls North_Carolina/
# manifest.json
# raleigh_property_boundaries.geojson
# wake_county_parcels.geojson
# ...
```

Each GeoJSON file is a standard `FeatureCollection`:

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "source_url": "https://services.arcgis.com/.../FeatureServer",
    "title": "Property Boundaries",
    "count": 100,
    "pulled_at": "2026-03-16T01:00:00"
  },
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-78.6, 35.7], ...]]
      },
      "properties": {
        "parcel_id": "0123456789",
        "owner_name": "SMITH, JOHN A",
        "address": "123 MAIN ST",
        "city": "RALEIGH",
        "zip": "27601",
        "acreage": 0.45,
        "land_use_normalized": "residential",
        "zoning_code": "R-4",
        "assessed_value": 285000,
        "land_value": 120000,
        "improvement_value": 165000,
        "year_built": 1978,
        "building_sq_ft": 2100,
        "source_url": "https://..."
      }
    }
  ]
}
```

### 3. Load into your tools

**Python (with geopandas):**
```python
import geopandas as gpd

# Load one file
gdf = gpd.read_file("North_Carolina/wake_county_parcels.geojson")
print(gdf.columns)
print(gdf[["owner_name", "address", "acreage", "land_use_normalized"]].head())

# Filter to residential over 5 acres
big_lots = gdf[(gdf["acreage"] > 5) & (gdf["land_use_normalized"] == "residential")]
big_lots.plot()
```

**QGIS:** File → Open → select any `.geojson` file. Style by `land_use_normalized` for a land use map.

**PostGIS:**
```bash
# Import into PostgreSQL/PostGIS
ogr2ogr -f "PostgreSQL" PG:"dbname=parcels" wake_county_parcels.geojson \
  -nln wake_county -overwrite
```

**MapLibre / Mapbox GL JS:**
```javascript
map.addSource('parcels', {
  type: 'geojson',
  data: 'wake_county_parcels.geojson'
});
map.addLayer({
  id: 'parcel-fill',
  type: 'fill',
  source: 'parcels',
  paint: { 'fill-color': '#088', 'fill-opacity': 0.4 }
});
```

**tippecanoe (vector tiles):**
```bash
# Convert to MBTiles for efficient map rendering
tippecanoe -o parcels.mbtiles -z14 -Z10 --drop-densest-as-needed \
  North_Carolina/*.geojson
```

## Normalized Schema

All data is normalized to a consistent schema regardless of source. See `SCHEMA.md` for the full spec.

**Key fields available (varies by source):**

| Field | Description | Availability |
|-------|-------------|-------------|
| `parcel_id` | APN/PIN/tax ID | ~95% of sources |
| `owner_name` | Property owner (public record) | ~60% of sources |
| `address` | Site/property address | ~55% of sources |
| `acreage` | Lot size in acres | ~50% of sources |
| `land_use_normalized` | residential/commercial/agricultural/etc | ~40% of sources |
| `zoning_code` | Zoning district code | ~25% of sources |
| `assessed_value` | Tax assessed value (USD) | ~30% of sources |
| `land_value` | Land-only value | ~25% of sources |
| `year_built` | Year structure was built | ~20% of sources |

## State manifests

Each state directory includes a `manifest.json`:

```json
{
  "state": "North Carolina",
  "sources_attempted": 127,
  "sources_successful": 89,
  "total_features": 8900,
  "pulled_at": "2026-03-16T...",
  "datasets": [
    {
      "title": "Wake County Parcels",
      "url": "https://...",
      "features": 100,
      "has_owner": true,
      "has_address": true,
      "has_acreage": true,
      "file": "wake_county_parcels.geojson"
    }
  ]
}
```

## Tools

### `discover_county_gis.py` — Find GIS endpoints

Searches ArcGIS Online API + known state portals to discover public parcel data services.

```bash
pip install httpx
python3 discover_county_gis.py
# Output: parcel_sources.json + PARCEL_DATA_SOURCES.md
```

### `scrape_gis_portals.py` — Deep discovery with Scrapling

Scrapes state GIS directory pages and does per-state ArcGIS searches. Deduplicates against existing index.

```bash
pip install httpx "scrapling[all]"
python3 scrape_gis_portals.py
# Adds new sources to parcel_sources.json
```

### `pull_all_states.py` — Download parcel data

Queries all indexed ArcGIS REST endpoints and saves normalized GeoJSON, organized by state.

```bash
pip install httpx
python3 pull_all_states.py
# Output: datasets/*.tar.gz (one per state)
```

## Data sources

| Source Type | Count | Description |
|-------------|-------|-------------|
| ArcGIS County/Local Services | ~14,600 | Direct REST API endpoints for county GIS |
| Statewide Parcel Portals | 26 | Free state-aggregated parcel datasets |
| Federal Overlays | 8 | NWI wetlands, NLCD land cover, FEMA floods, USDA crops |

### Free statewide parcel data (26 states)

Arkansas, Colorado, Connecticut, Delaware, Idaho, Indiana, Iowa, Maine, Maryland, Massachusetts, Michigan, Minnesota, Mississippi, Montana, Nebraska, New Jersey, New Mexico, North Carolina, Oregon, Pennsylvania, South Carolina, Utah, Vermont, Virginia, Wisconsin, Wyoming

### Federal overlay data

- **National Wetlands Inventory (NWI)** — wetland boundaries & classification
- **NLCD Land Cover** — forest, urban, water, cropland (30m raster)
- **USDA Cropland Data Layer** — crop-specific land cover
- **FEMA Flood Zones (NFHL)** — flood hazard areas
- **PAD-US Protected Areas** — federal/state/local protected lands
- **BLM PLSS** — township/range/section grid (western states)
- **Census TIGER/Line** — county/tract boundaries
- **USGS National Map** — elevation, hydro, base layers

## How the field mapping works

Every county names their fields differently. The puller maps 30+ naming conventions per field:

| Our Field | Source Names It Matches |
|-----------|----------------------|
| `owner_name` | OWNER, OWNER1, OWNERNAME, GRANTEE, TAXPAYER, OWN_NAME, ... |
| `address` | SITEADDR, SITE_ADDR, PROP_ADDR, LOCATION, SITUS, ... |
| `acreage` | ACRES, ACREAGE, CALCACRES, GIS_ACRES, LOT_ACRES, ... |
| `assessed_value` | ASSESSED, TOTAL_AV, TOTALVAL, ASSESSEDVALUE, ... |
| `parcel_id` | APN, PIN, PID, PARCEL_ID, TAX_ID, FOLIO, ... |

See `SCHEMA.md` for the complete mapping table.

## Limitations

- **Sample data**: Each source is capped at 100 features per pull (enough to validate the source). For full county coverage, increase `MAX_FEATURES_PER_SOURCE` in `pull_all_states.py` or use the ArcGIS REST API directly with pagination.
- **Not all sources have all fields**: Owner name, zoning, and values depend on what each county publishes. The `manifest.json` per state tells you which sources have which fields.
- **Data freshness**: Pulled from live APIs — data is as current as the county's GIS system. Re-run the puller to refresh.
- **Geometry**: All coordinates are WGS84 (EPSG:4326). Some sources may have simplified geometries.

## Scaling up

To pull full datasets (all parcels, not just 100 per source):

```python
# In pull_all_states.py, change:
MAX_FEATURES_PER_SOURCE = 100  # → 50000 or more

# Or query a specific endpoint directly:
curl "https://services.arcgis.com/.../FeatureServer/0/query?\
where=1%3D1&outFields=*&f=geojson&resultRecordCount=2000&resultOffset=0&outSR=4326"
```

Most ArcGIS services cap at 1000-2000 features per request. Paginate with `resultOffset` to get everything.

## License

The source data is public government records. This repo's tools are MIT licensed.
