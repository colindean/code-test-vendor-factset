package cx.cad.codetests.vendor_factset

import cx.cad.codetests.vendor_factset.CsvTools.Row
import cx.cad.codetests.vendor_factset.Data.fileFor

import scala.collection.GenSeq
import scala.collection.parallel.{ParIterable, ParSeq}
import scala.collection.parallel.immutable.ParMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object FactsetData {
  lazy val entities: GenSeq[Entity] = {
    val resultSet = SqlTools.query("SELECT * FROM entity_coverage")
    val data: ParSeq[Row] = SqlTools.resultSetToMap(resultSet).par

    val output = for {
      coverage <- data
      id <- coverage.get(FieldNames.Factset.Entity.Id)
      entityId = EntityId(id)
      /*addressResultSet = SqlTools.query(s"""SELECT * from entity_address WHERE factset_entity_id = "${id}"""") // skipping prepared statements for now
      addresses = SqlTools.resultSetToMap(addressResultSet)
      structureResultSet = SqlTools.query(s"""SELECT * from entity_structure WHERE factset_entity_id = "${id}"""") // skipping prepared statements for now
      structure = SqlTools.resultSetToMap(structureResultSet)*/

    } yield Entity(
      id = entityId,
      coverageData = coverage,
      addressData = List.empty, //addresses.toList,
      structureData = List.empty, //structure.toList
    )
    println(s"Got ${output.size} entities…")
    output
  }
}

object FactsetDataCsv {
  import scala.concurrent.ExecutionContext.Implicits.global

  lazy val factset_address_data = Future { CsvTools.read(fileFor("factset__ent_entity_address.csv")) }
  lazy val factset_structure_data = Future { CsvTools.read(fileFor("factset__ent_entity_structure.csv")) }
  lazy val factset_coverage_data = Future { CsvTools.read(fileFor("factset__ent_entity_coverage.csv")) }

  lazy val futurefactsetAddressesByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_address_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))
  lazy val futurefactsetCoverageByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_coverage_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))
  lazy val futurefactsetStructureByEntityId: Future[ParMap[String, ParSeq[Map[String, String]]]] = factset_structure_data.map(_.par.groupBy(_(FieldNames.Factset.Entity.Id)))

  lazy val entities2: ParIterable[Entity] = Await.result({for {
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
