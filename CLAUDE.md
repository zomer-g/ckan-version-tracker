# OVER — project rules

## One dataset per table

A tracked source never holds several tables side by side in one dataset (a
register with its lots, bids, parcels and documents index as separate tables of
one dataset). Each table is a dataset of its own, with its own versions.

What ties the datasets of one source together is the source itself (same
source, chip and organisation) and, where useful, a shared tag — not a shared
dataset.

New sources are defined in GOVSCRAPER (see its CLAUDE.md, and michrazim as the
worked example). A dataset may still carry the same rows in more than one file
format (CSV + GeoJSON/GPKG) and the files those rows point to.
