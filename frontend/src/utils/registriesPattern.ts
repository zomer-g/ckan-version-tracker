/**
 * Single source of truth for the "is this a registries.health.gov.il
 * URL we know how to scrape?" check on the frontend. Used by HomePage
 * and SearchPage to gate the "auto-treat as scraper" code path when the
 * user pastes a URL instead of typing a search query.
 *
 * Mirrors the registry catalog in ``app/api/registries.py``: only
 * per-registry URLs ``/<RegistryPath>`` for a known registry are in
 * scope. The bare site root (the registry selector) and unknown paths
 * are rejected — each registry is tracked as its own dataset.
 *
 * The backend is the authoritative validator; this regex is only a
 * quick UI hint so the search bar treats the pasted URL as a scraper
 * candidate instead of falling through to keyword search.
 */

export const REGISTRIES_PATTERN =
  /^https?:\/\/registries\.health\.gov\.il\/(Ambulances|Paramedics|FoodImporters|FoodManufacturers|RadiationInstitutes|CosmeticsBusinesses|FoodFactories|MedicalDevices|Institutions|Cosmetics)\/?(?:[?#].*)?$/i;
