require 'pmap'
require 'csv'

def clean_row(row)
  output = {}
  row.to_hash.each_pair do |k, v|
    # clean whitepace
    value = v.strip unless v.nil?
    # TODO: consider other cleaning, e.g. parse to Number, etc.
    output[k.to_sym] = value
  end
  output
end

geo_data = CSV.open(File.new('data/mdl__dim_geo.csv'), headers: true).pmap { |row| clean_row(row) }
vendor_data = CSV.open(File.new('data/mdl__dim_vendor.csv'), headers: true).pmap { |row| clean_row(row) }

geos_by_geo_id = geo_data.group_by { |geo| geo[:geo_id] }
vendors_by_vendor_id = vendor_data.group_by { |vendor| vendor[:vendor_id] }

Vendor = Struct.new(:id, :geo, :vendor) do
  include Comparable
  def <=>(other)
    id <=> other.id
  end
end

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

factset_address_data = CSV.open(File.new('data/factset__ent_entity_address.csv'), headers: true).pmap { |row| clean_row(row) }
factset_structure_data = CSV.open(File.new('data/factset__ent_entity_structure.csv'), headers: true).pmap { |row| clean_row(row) }
factset_coverage_data = CSV.open(File.new('data/factset__ent_entity_coverage.csv'), headers: true).pmap { |row| clean_row(row) }

factset_addresses_by_entity_id = factset_address_data.group_by { |thing| thing[:factset_entity_id] }
factset_structure_by_entity_id = factset_structure_data.group_by { |thing| thing[:factset_entity_id] }
factset_coverage_by_entity_id  = factset_coverage_data.group_by { |thing| thing[:factset_entity_id] }

Entity = Struct.new(:id, :address, :structure, :coverage) do
  include Comparable
  def <=>(other)
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

# Now, try to link a vendor to an entity.

require 'jaro_winkler'

NAME_CHECK_JARO_WINKLER_DISTANCE_THRESHOLD = 0.98
DEBUG = false
check_names = proc do |entity, vendor|
  output = []
  entity_names = [entity.coverage[:entity_name], entity.coverage[:entity_proper_name]]
  vendor_name = vendor.vendor[:name]
  capitalized = vendor_name.upcase
  no_punctuation = capitalized.gsub(/[^[:word:]\s]/, '')
  variants = [vendor_name, capitalized, no_punctuation]
  STDERR.puts "Is #{entity_names} in #{variants}?" if DEBUG
  entity_names.find do |name|
    if variants.include? name
      hash = { factset_entity_id: entity.id, vendor_id: vendor.id, confidence: 1.0, reason: 'name ~exact match' }
      STDERR.puts "EXACT MATCH! #{hash}"
      output << hash
    else
      jw_dist = JaroWinkler.distance entity.coverage[:entity_name], no_punctuation
      if jw_dist > NAME_CHECK_JARO_WINKLER_DISTANCE_THRESHOLD
        hash = { factset_entity_id: entity.id, vendor_id: vendor.id, confidence: jw_dist, reason: "name jaro winkler exceeding #{NAME_CHECK_JARO_WINKLER_DISTANCE_THRESHOLD}" }
        STDERR.puts "JW hit: #{hash}"
        output << hash
      end
    end
  end
  output
end

checks = [check_names]
matches = []
entities.each do |entity|
  vendors.select do |vendor|
    checks.each do |check|
      result = check.call(entity, vendor)
      STDERR.puts "Found #{result.size} results…" if DEBUG
      matches += result
    end
  end
end

CSV.open('matches.csv', 'w', headers: matches.first.keys, write_headers: true) do |csv|
  matches.each do |match|
    csv << match.values
  end
end
