# US Land & Parcel Data Viewer — App Build Instructions

## What to build

A web app that displays US property parcel boundaries on an interactive map with owner data, acreage, zoning, land use, and property values.

## Core Features

1. **Interactive map** — full US coverage, smooth zoom/pan (MapLibre GL JS or Mapbox GL)
2. **User location** — start the map centered on the user's GPS location (browser geolocation API)
3. **Address search** — type an address to fly to that location on the map (geocoding)
4. **Parcel boundaries** — colored polygon overlays showing property lot lines
5. **Click-to-inspect** — click any parcel to see a popup/sidebar with:
   - Owner name
   - Site address
   - Acreage / square footage
   - Land use (residential, commercial, agricultural, vacant, etc.)
   - Zoning code and description
   - Assessed value, land value, improvement value
   - Year built, building sq ft
6. **Owner search** — search by owner name across all parcels
7. **Filter/search by data type** — filter parcels by land use, zoning, acreage range, value range
8. **Layer toggles** — toggle overlay layers: wetlands, flood zones, land cover, cropland

## Data Source

All data is pre-pulled and available at: **https://github.com/lukewrightmain/parcel-index-sites**

### What's in the repo:
- `datasets/` — 52 compressed archives (one per state), containing GeoJSON files with parcel polygons + attributes
- `parcel_sources.json` — index of 14,660 live ArcGIS REST API endpoints (can query for more data in real-time)
- `manifests/` — per-state JSON files listing every data source and what fields it has
- `SCHEMA.md` — normalized field schema

### GeoJSON format (what the data looks like):
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-78.6, 35.7], [-78.6, 35.8], [-78.5, 35.8], [-78.5, 35.7], [-78.6, 35.7]]]
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
        "building_sq_ft": 2100
      }
    }
  ]
}
```

### Two data approaches (pick one or combine):

**Option A — Static (simpler):** Download the GeoJSON files from the repo, convert to vector tiles (PMTiles format using tippecanoe), host the .pmtiles file statically. No backend needed.

**Option B — Live API proxy (richer):** Query the ArcGIS REST endpoints from `parcel_sources.json` in real-time as the user pans the map. Endpoint pattern:
```
{service_url}/{layer_id}/query?where=1=1&geometry={bbox}&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&outFields=*&f=geojson&outSR=4326
```

## Tech Stack

- **Frontend:** SvelteKit or Next.js
- **Map:** MapLibre GL JS (free, open-source) with vector tiles
- **Geocoding:** Nominatim (free) or Mapbox Geocoding API
- **Tile format:** PMTiles (static, no tile server needed) or Martin (PostGIS → vector tiles)
- **Database (if using Option B):** PostGIS (PostgreSQL + spatial extension)
- **Hosting:** Vercel, Cloudflare Pages, or any static host (for Option A)

## UI Layout

```
┌──────────────────────────────────────────────────┐
│  🔍 [Search address or owner...]    [Filters ▼]  │
├──────────────────────────────────────────────────┤
│                                    │ Parcel Info  │
│                                    │              │
│           INTERACTIVE MAP          │ Owner: ...   │
│        (full width, parcels        │ Address: ... │
│         shown as polygons)         │ Acres: ...   │
│                                    │ Zoning: ...  │
│                                    │ Value: $...  │
│                                    │ Land Use:... │
│         [📍 My Location]           │              │
│                                    │ [More Data]  │
└──────────────────────────────────────────────────┘
```

- Map takes ~75% width, info panel ~25% (collapsible on mobile)
- Bottom bar or floating panel for layer toggles
- Color parcels by land use type (green=agricultural, yellow=residential, blue=commercial, gray=vacant)
- Mobile: full-screen map, bottom sheet for parcel info

## Key Implementation Notes

- Use `navigator.geolocation.getCurrentPosition()` for initial map center
- Parcels render as vector tile fill+line layers (not individual GeoJSON — too heavy at scale)
- Click handler: `map.on('click', 'parcels-fill', (e) => { show sidebar with e.features[0].properties })`
- Address search: geocode to lat/lng, then `map.flyTo({center: [lng, lat], zoom: 17})`
- Owner search: query PostGIS `WHERE owner_name ILIKE '%search%'` or client-side filter
- For 500K+ features, PMTiles is the right format (not raw GeoJSON — browsers will crash)

## PMTiles conversion (run once to prep data)

```bash
# Install tippecanoe
brew install tippecanoe  # or build from source

# Convert all GeoJSON to one PMTiles file
tippecanoe -o us_parcels.pmtiles \
  -z16 -Z8 \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  datasets/extracted/*/*.geojson

# Host the .pmtiles file on any static host (S3, Cloudflare R2, GitHub Releases)
```

## Color scheme for land use

```javascript
const landUseColors = {
  residential:  '#FFEB3B',  // yellow
  commercial:   '#2196F3',  // blue
  industrial:   '#9C27B0',  // purple
  agricultural: '#4CAF50',  // green
  vacant:       '#9E9E9E',  // gray
  government:   '#F44336',  // red
  other:        '#FF9800',  // orange
};
```
