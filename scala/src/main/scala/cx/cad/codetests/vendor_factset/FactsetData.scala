package cx.cad.codetests.vendor_factset

import cx.cad.codetests.vendor_factset.Data.fileFor

import scala.collection.parallel.{ParIterable, ParSeq}
import scala.collection.parallel.immutable.ParMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object FactsetData {
  import scala.concurrent.ExecutionContext.Implicits.global

  lazy val factset_address_data = Future { CsvTools.read(fileFor("factset__ent_entity_address.csv")) }
  lazy val factset_structure_data = Future { CsvTools.read(fileFor("factset__ent_entity_structure.csv")) }
  lazy val factset_coverage_data = Future { CsvTools.read(fileFor("factset__ent_entity_coverage.csv")) }

  lazy val futurefactsetAddressesByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_address_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))
  lazy val futurefactsetCoverageByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_coverage_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))
  lazy val futurefactsetStructureByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_structure_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))

  lazy val entities: ParIterable[Entity] = Await.result({for {
    factsetCoverageByEntityId <- futurefactsetCoverageByEntityId
    factsetStructureByEntityId <- futurefactsetStructureByEntityId
    factsetAddressesByEntityId <- futurefactsetAddressesByEntityId
  } yield for {
    id <- factsetCoverageByEntityId.keys.par
    coverage <- factsetCoverageByEntityId.get(id)
    structure <- factsetStructureByEntityId.get(id)
    addresses <- factsetAddressesByEntityId.get(id)
  } yield
    {
      print("E")
      Entity(
        EntityId(id),
        coverageData = coverage.toList.head,
        structureData = structure.toList,
        addressData = addresses.toList
      )
    }}, Duration.Inf) // this is awful

}
