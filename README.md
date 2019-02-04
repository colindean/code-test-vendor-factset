# Code Test: vendor-factset

_All code in this repository is licensed under the terms of the [GNU Affero General Public License v3 or later](https://spdx.org/licenses/AGPL-3.0-or-later.html)._

This is the result of an interview code test and should not be used for anything serious.

## Goal

Given five tables of data describing two different sets of data with disparate unique identifiers for each type, vendors and factsets, associate a vendor id with a factset id. The mapping will generally be 1:1 but it is possible for it to be 1:N.

## Research

The actual data files are not included in this repository because I have not been given permission to share them, nor have I asked!

### Fields

mdl_dim_vendor fields:

```
vendor_id	parent_vendor_id	top_vendor_id	cnt_children	orgtype_id	geo_id	name	email	phone	fax	dunsnumber	websiteurl	address	address1	address2	country	zipcode	parentdunsnumber	score	cnt_opp	bucket_id	load_date	lvl
```

mdl_dim_geo fields:

```
geo_id	zipcode	is_primary	latitude	longitude	elevation	state	state_full_name	area_code	city	city_display	county	county_fips	state_fips	timezone	daylight_saving	region	division	congress_district	congress_land_area	country	continent	country_iso2
```

factset_entity_coverage fields:

```
factset_entity_id	entity_name	entity_proper_name	primary_sic_code	industry_code	sector_code	iso_country	metro_area	state_province	zip_postal_code	web_site	entity_type	entity_sub_type	year_founded	iso_country_incorp	iso_country_cor	nace_code
```

factset_entity_address fields:

```
address_id	factset_entity_id	location_city	state_province	location_postal_code	city_state_zip	location_street1	location_street2	location_street3	iso_country	tele_country	tele_area	tele	tele_full	fax_country	fax_area	fax	fax_full	hq
```

factset_entity_structure fields:

```
factset_entity_id	factset_parent_entity_id	factset_ultimate_parent_entity_id
```

### Data Cleanliness

The data was provided in CSV format, but the format was not clean: 

* In `mdl_dim_vendor.csv`, there are rows with spurious newlines in them. I caught this by running `cat -n data/mdl__dim_vendor.csv | cut -d , -f 1 | awk '{print $1 ":" $2}' | grep -v "\d.*:\d.*"` and manually fixing the broken records given the line numbers.
* `mdl__dim_geo.csv` and `factset__ent_entity_address` seemed to be cleanly formatted when run through that same command.
* `factset__ent_entity_structure` seemed to be structured data already with no chance to break newlines, etc. so I did not evaluate its cleanliness beyond a quick visual pass.
* `factset__ent_entity_coverage` seemed to be clean with a quick validation of its first column, comprised of six alphanumeric characters followed by `-E`: `cat -n data/factset__ent_entity_coverage.csv | cut -d , -f 1 | awk '{print $1 ":" $2}' | grep -v '\d.*:.*-E'`.

It's possible that there could be other errors in here but I'm leaving them for now!

_(Yeah, I could have made those commands a little more succinct but they ran fast enough for this exercise.)_

