package cx.cad.codetests.vendor_factset

import cx.cad.codetests.vendor_factset.CsvTools.Row

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
case class Entity(id: EntityId,
                  coverageData: Row, // only one
                  addressData: List[Row],
                  structureData: List[Row]
                 )
case class EntityId(id: String) extends AnyVal

case class Vendor(id: VendorId,
                  vendorData: Row,
                  geoData: List[Row]
                 )
case class VendorId(id: String) extends AnyVal


object FieldNames {
  object Vendor {
    val Id = "vendor_id"
    val GeoId = "geo_id"
  }
  object Factset {
    object Entity {
      val Id = "factset_entity_id"
      val Name = "entity_name"
      val ProperName = "entity_proper_name"
    }
  }
}