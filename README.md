# Code Test: vendor-factset

_All code in this repository is licensed under the terms of the [GNU Affero General Public License v3 or later](https://spdx.org/licenses/AGPL-3.0-or-later.html)._

This is the result of an interview code test and should not be used for anything serious.

## Goal

Given five tables of data describing two different sets of data with disparate unique identifiers for each type, vendors and factsets, associate a `vendor_id` with a `factset_entity_id`. The mapping will generally be 1:1 but it is possible for it to be 1:N.

## Research

The actual data files are not included in this repository because I have not been given permission to share them, nor have I asked!

### Fields

#### Vendors

mdl_dim_vendor fields:

```
vendor_id	parent_vendor_id	top_vendor_id	cnt_children	orgtype_id	geo_id	name	email	phone	fax	dunsnumber	websiteurl	address	address1	address2	country	zipcode	parentdunsnumber	score	cnt_opp	bucket_id	load_date	lvl
```

mdl_dim_geo fields:

```
geo_id	zipcode	is_primary	latitude	longitude	elevation	state	state_full_name	area_code	city	city_display	county	county_fips	state_fips	timezone	daylight_saving	region	division	congress_district	congress_land_area	country	continent	country_iso2
```

It appears that there's a 1:N relationship of Geo:Vendor.

#### Factsets

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

### Analysis

Model the data

```ruby
require 'pmap'
require 'csv'

def clean_row(row)
  output = {}
  row.to_hash.each_pair do |k,v| 
    # clean whitepace
    value = v.strip if !v.nil?
    # TODO: consider other cleaning, e.g. parse to Number, etc.
    output[k.to_sym] = value
  end
  output
end

geo_data = CSV.open(File.new("data/mdl__dim_geo.csv"), {headers: true}).pmap { |row| clean_row(row) } 
vendor_data = CSV.open(File.new("data/mdl__dim_vendor.csv"), {headers: true}).pmap { |row| clean_row(row) } 

geos_by_geo_id = geo_data.group_by { |geo| geo[:geo_id] }
vendors_by_vendor_id = vendor_data.group_by { |vendor| vendor[:vendor_id] }

Vendor = Struct.new(:id, :geo, :vendor) do 
  include Comparable
  def <=> other
    id <=> other.id
  end
end

# WIP!
vendors = vendors_by_vendor_id.pmap do |id_vendor|
  begin
    vendor = Vendor.new
    vendor.id = id_vendor[0]
    vendor.vendor = id_vendor[1].first
    vendor.geo = geos_by_geo_id[vendor.vendor[:geo_id]].first if vendor.vendor[:geo_id] && geos_by_geo_id[vendor.vendor[:geo_id]]
    vendor
  rescue NoMethodError
    puts "Vendor was #{id_vendor}"
    sleep 1
  end
end

factset_address_data = CSV.open(File.new("data/factset__ent_entity_address.csv"), {headers: true}).pmap { |row| clean_row(row) }
factset_structure_data = CSV.open(File.new("data/factset__ent_entity_structure.csv"), {headers: true}).pmap { |row| clean_row(row) }
factset_coverage_data = CSV.open(File.new("data/factset__ent_entity_coverage.csv"), {headers: true}).pmap { |row| clean_row(row) }

factset_addresses_by_entity_id = factset_address_data.group_by { |thing| thing[:factset_entity_id] }
factset_structure_by_entity_id = factset_structure_data.group_by { |thing| thing[:factset_entity_id] }
factset_coverage_by_entity_id  = factset_coverage_data.group_by { |thing| thing[:factset_entity_id] }

Entity = Struct.new(:id, :address, :structure, :coverage) do
  include Comparable
  def <=> other
    id <=> other.id
  end
end

entities = factset_coverage_by_entity_id.pmap do |id_entity|
  entity = Entity.new
  entity.id = id_entity[0]
  entity.coverage = id_entity[1].first
  entity.structure = factset_structure_by_entity_id[entity.id].first if factset_structure_by_entity_id[entity.id]
  entity.address = factset_addresses_by_entity_id[entity.id] # seems like there could be more than one
  entity
end

```

Now, try to link a vendor to an entity.
