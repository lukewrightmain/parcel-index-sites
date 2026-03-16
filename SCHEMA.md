# Parcel Data Schema

Standard format for all ingested parcel data, regardless of source county/state.

## Source Index Schema (`parcel_sources.json`)

Each entry in the source index:

```json
{
  "source_id": "string — unique identifier (state_county or arcgis_id)",
  "source": "statewide | federal | arcgis_online | scraped",
  "state": "string — full state name",
  "county": "string — county name (if county-level)",
  "fips_code": "string — 5-digit FIPS (2 state + 3 county)",
  "url": "string — main portal URL",
  "parcels_url": "string — direct link to parcel data/API",
  "api_type": "arcgis_rest | wms | wfs | shapefile | geojson | csv",
  "format": "string — data format description",
  "has_parcels": "boolean — confirmed parcel boundaries",
  "has_ownership": "boolean — includes owner name",
  "has_zoning": "boolean — includes zoning codes",
  "has_land_use": "boolean — includes land use classification",
  "has_acreage": "boolean — includes area/acreage",
  "has_value": "boolean — includes assessed/market value",
  "verified": "boolean — endpoint responded",
  "last_verified": "ISO date — when last checked",
  "views": "number — ArcGIS view count (popularity proxy)",
  "notes": "string — human-readable notes"
}
```

## Normalized Parcel Schema (`parcels/`)

Every parcel from every source gets normalized to this schema before storage.
Stored as GeoJSON FeatureCollection, one file per county (`{fips_code}.geojson`).

```json
{
  "type": "Feature",
  "geometry": {
    "type": "Polygon | MultiPolygon",
    "coordinates": "GeoJSON coordinates (WGS84 / EPSG:4326)"
  },
  "properties": {
    // ── Identity ──
    "parcel_id": "string — source parcel ID (APN, PIN, PID, etc.)",
    "fips_code": "string — 5-digit county FIPS",
    "state": "string — state name",
    "county": "string — county name",

    // ── Location ──
    "address": "string — full site address (if available)",
    "city": "string",
    "zip": "string",
    "latitude": "number — centroid lat",
    "longitude": "number — centroid lon",

    // ── Ownership ──
    "owner_name": "string — current owner (public record)",
    "owner_address": "string — mailing address",
    "owner_type": "private | corporate | government | trust | unknown",

    // ── Land Details ──
    "acreage": "number — total acres",
    "sq_ft": "number — total square feet",
    "land_use_code": "string — raw code from source",
    "land_use": "string — normalized: residential | commercial | industrial | agricultural | vacant | government | mixed | other",
    "zoning_code": "string — raw zoning code",
    "zoning": "string — normalized: R1 | R2 | C1 | C2 | I1 | AG | etc.",
    "zoning_description": "string — human-readable zoning description",

    // ── Classification (from federal overlays) ──
    "nlcd_class": "string — NLCD land cover (forest, urban, water, etc.)",
    "is_wetland": "boolean — NWI wetland overlay",
    "wetland_type": "string — NWI classification code",
    "flood_zone": "string — FEMA flood zone (A, AE, X, etc.)",
    "is_farmland": "boolean — USDA cropland layer",
    "crop_type": "string — USDA crop classification",
    "is_protected": "boolean — PAD-US protected area",
    "protection_type": "string — federal, state, local, private",

    // ── Valuation ──
    "assessed_value": "number — assessed value (USD)",
    "land_value": "number — land-only value (USD)",
    "improvement_value": "number — improvement/building value (USD)",
    "market_value": "number — fair market value (USD)",
    "tax_year": "number — assessment year",

    // ── Structure ──
    "has_structure": "boolean",
    "year_built": "number",
    "building_sq_ft": "number",
    "bedrooms": "number",
    "bathrooms": "number",

    // ── Metadata ──
    "source_url": "string — where this record came from",
    "source_updated": "ISO date — when source last updated",
    "ingested_at": "ISO date — when we pulled it",
    "raw_data": "object — original fields from source (for debugging)"
  }
}
```

## Overlay Data Schema

Federal overlay data stored as separate layers, joined by spatial intersection.

| Layer | Source | Format | Resolution |
|-------|--------|--------|------------|
| Land Cover | NLCD (mrlc.gov) | GeoTIFF raster | 30m |
| Wetlands | NWI (FWS) | Vector polygons | Survey-grade |
| Flood Zones | FEMA NFHL | Vector polygons | Varies |
| Cropland | USDA CDL | GeoTIFF raster | 30m |
| Protected Areas | PAD-US | Vector polygons | Survey-grade |
| PLSS Grid | BLM | Vector polygons | Survey-grade |

## Storage Layout

```
data/
├── parcel_sources.json          # Source index (all GIS endpoints)
├── PARCEL_DATA_SOURCES.md       # Human-readable index
├── SCHEMA.md                    # This file
├── parcels/                     # Normalized parcel data
│   ├── 01001.geojson           # Autauga County, AL (by FIPS)
│   ├── 01003.geojson           # Baldwin County, AL
│   └── ...
├── overlays/                    # Federal overlay data
│   ├── nlcd/                   # Land cover rasters
│   ├── nwi/                    # Wetland polygons
│   ├── fema/                   # Flood zones
│   └── cropland/               # USDA crop data
└── raw/                        # Raw downloaded source files
    ├── shapefiles/
    ├── geojson/
    └── csv/
```

## Field Mapping Guide

Every source uses different field names. Map them to our schema:

| Our Field | Common Source Names |
|-----------|--------------------|
| parcel_id | APN, PIN, PID, PARCEL_ID, PARCELID, PARCELNB, TAX_ID, ACCOUNT_NO |
| owner_name | OWNER, OWNER1, OWNERNAME, OWNER_NAME, GRANTEE, TAXPAYER |
| address | SITEADDR, SITE_ADDR, PROP_ADDR, ADDRESS, LOCATION, SITUS |
| acreage | ACRES, ACREAGE, AREA_ACRES, CALCACRES, GIS_ACRES, LANDAREA |
| land_use | LANDUSE, LAND_USE, USE_CODE, PROP_CLASS, CLASS_CODE, LUC |
| zoning | ZONING, ZONE, ZONE_CODE, ZONING_CODE, ZONEDESC |
| assessed_value | ASSESSED, ASSD_VAL, TOTAL_AV, ASSESSED_VALUE, TOTALVAL |
| land_value | LANDVAL, LAND_VAL, LAND_VALUE, LANDASSD |
| year_built | YEARBUILT, YR_BUILT, YEAR_BUILT, YRBUILT, EFFYRBUILT |
| sq_ft | SQFT, SQ_FT, BLDG_SQFT, TOTSQFT, GROSSAREA, LIVEAREA |
