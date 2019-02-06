package cx.cad.codetests.vendor_factset

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.github.vickumar1981.stringdistance.StringConverter._
import cx.cad.codetests.vendor_factset.CsvTools.Row
import cx.cad.codetests.vendor_factset.Data.fileFor

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object Main extends App {

  val activeChecks: List[Check] = List(NameChecker)

  println(s"Executing ${activeChecks.size} checks…")

  val checkResults = for {
    entity <- FactsetData.entities
    vendor <- VendorData.vendors
    check  <- activeChecks
  } yield check.check(entity, vendor)


  val f = new File("out.csv")
  val writer = CSVWriter.open(f)
  //headers
  writer.writeRow(CheckMatch.headers)
  checkResults.toStream.foreach {
    case Some(checkMatch) => writer.writeRow(checkMatch.asCsvList)
    case None =>
  }
  writer.close()
}
trait Check {
  def check(entity: Entity, vendor: Vendor): Option[CheckMatch]
}
object NameChecker extends Check {
  val NameCheckJaroWinklerDistanceThreshold = 0.98

  override def check(entity: Entity, vendor: Vendor): Option[CheckMatch] = {
    val entityNames = List(
      entity.coverageData(CsvConstants.Factset.Entity.Name),
      entity.coverageData(CsvConstants.Factset.Entity.ProperName))

    val vendorName = vendor.vendorData("name")
    val vendorNameNoPunct = removePunctuation(vendorName)
    val variants = List(
      vendorName,
      vendorNameNoPunct,
      vendorName.toUpperCase,
    )
    // exact matches
    val matches = entityNames.intersect(variants)

    if(matches.nonEmpty) {
      val result = CheckMatch(
        entityId = entity.id,
        vendorId = vendor.id,
        confidence = 1.0,
        reason = s"Name exact match ($matches)")

      println(s"EXACT MATCH: $result")
      return Some(result)
    }

    // try string differencing
    //val jwDist: Double = JaroWinkler.score(
    //  entityNames.head, vendorNameNoPunct)
    //implicit works better?? wtf
    val jwDist: Double = entityNames.head.jaroWinkler(vendorNameNoPunct)

    if(jwDist > NameCheckJaroWinklerDistanceThreshold) {
      val result = CheckMatch(
        entityId = entity.id,
        vendorId = vendor.id,
        confidence = jwDist,
        reason = s"Name jw exceeding ${NameCheckJaroWinklerDistanceThreshold}"
      )
      println(s"JW MATCH: (${result})")
      return Some(result)
    }

    None
  }

  def removePunctuation(string: String): String = {
    string.replaceAll("/[^[:word:]\\s]/", "")
  }
}

case class CheckMatch(
            vendorId: VendorId,
            entityId: EntityId,
            confidence: Double,
            reason: String) {
  def asCsvList: List[String] = List(vendorId.id, entityId.id, confidence.toString, reason)
}
object CheckMatch {
  // ideally, this should come from some kind of introspection on the case class
  // not going to dig into that right now
  def headers = List("vendorId", "entityId", "confidence", "reason")
}

object Data {
  def fileFor(filename: String) = new File(s"../data/${filename}")
}

object VendorData {
  lazy val geo_data = CsvTools.read(fileFor("mdl__dim_geo.csv"))
  lazy val vendor_data = CsvTools.read(fileFor("mdl__dim_vendor.csv"))

  lazy val geosByVendorId = geo_data.groupBy(_(CsvConstants.Vendor.Id))
  lazy val vendorsByVendorId = vendor_data.groupBy(_(CsvConstants.Vendor.Id))

  lazy val vendors = vendorsByVendorId.par.map { case (vendorId: String, data: Seq[Row]) => {
    val geos: List[Row] = geosByVendorId.getOrElse(vendorId, List.empty)
    Vendor(
      id = VendorId(vendorId),
      vendorData = data.head,
      geoData = geos

    )
  }}
}

case class Vendor(id: VendorId,
                  vendorData: Row,
                  geoData: List[Row]
                 )
case class VendorId(id: String) extends AnyVal

object FactsetData {
  lazy val factset_address_data = CsvTools.read(fileFor("factset__ent_entity_address.csv"))
  lazy val factset_structure_data = CsvTools.read(fileFor("factset__ent_entity_structure.csv"))
  lazy val factset_coverage_data = CsvTools.read(fileFor("factset__ent_entity_coverage.csv"))

  lazy val factsetAddressesByEntityId = factset_address_data.par.groupBy(_(CsvConstants.Factset.Entity.Id))
  lazy val factsetCoverageByEntityId = factset_coverage_data.par.groupBy(_(CsvConstants.Factset.Entity.Id))
  lazy val factsetStructureByEntityId = factset_structure_data.par.groupBy(_(CsvConstants.Factset.Entity.Id))

  lazy val entities = for {
    id <- factsetCoverageByEntityId.keys.par
    coverage <- factsetCoverageByEntityId.get(id)
    structure <- factsetStructureByEntityId.get(id)
    addresses <- factsetAddressesByEntityId.get(id)
  } yield
    Entity(
      EntityId(id),
      coverageData = coverage.toList.head,
      structureData = structure.toList,
      addressData = addresses.toList
    )
}

case class Entity(id: EntityId,
                  coverageData: Row, // only one
                  addressData: List[Row],
                  structureData: List[Row]
                 )
case class EntityId(id: String) extends AnyVal

object CsvConstants {
  object Vendor {
    val Id = "vendor_id"
  }
  object Factset {
    object Entity {
      val Id = "factset_entity_id"
      val Name = "entity_name"
      val ProperName = "entity_proper_name"
    }
  }
}

object CsvTools {
  type Row = Map[String, String]

  def read(path: File): List[Map[String, String]] = {
    val reader = CSVReader.open(path)
    reader.allWithHeaders()
  }
  def cleanRow(row: Row): Row = {
    row.map { case (field: String, value: String) =>
      field -> value.trim()
    }
  }
}