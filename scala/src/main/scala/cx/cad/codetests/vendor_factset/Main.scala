package cx.cad.codetests.vendor_factset

import java.io.File

import com.github.tototoshi.csv.CSVWriter

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object Main extends App {

  val activeChecks: List[Check] = List(NameChecker)

  println(s"Executing ${activeChecks.size} checks…")

  val f = new File("out.csv")
  val writer = CSVWriter.open(f)
  //headers
  writer.writeRow(CheckMatch.headers)

  for {
    entity <- FactsetData.entities
    vendor <- VendorData.vendors
    check  <- activeChecks
    success <- check.check(entity, vendor)
  } yield writer.writeRow(success.asCsvList)

  writer.close()
}



