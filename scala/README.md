# Vendor-Factset (Scala version)

## Preparation

You'll need to create a database out of the several CSV files _after_ cleaning
them according to the instructions in the README in the parents directory.

Open the database console while _in the parent directory_ with:

    sqlite3 data/db.db

Then import the data from CSV:

    .import data/factset__ent_entity_structure.csv entity_structure
    .import data/factset__ent_entity_address.csv entity_address
    .import data/factset__ent_entity_coverage.csv entity_coverage
    .import data/mdl__dim_geo.csv vendor_geo
    .import data/mdl__dim_vendor.csv vendor_vendor

To be sure that you've done it correctly, check the output of

    .schema vendor_vendor

and some other tables.

## Running

    sbt run

No effort was expended in making this run outside of `sbt` executed from within
this directory.
