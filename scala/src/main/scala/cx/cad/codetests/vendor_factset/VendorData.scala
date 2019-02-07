package cx.cad.codetests.vendor_factset

import cx.cad.codetests.vendor_factset.CsvTools.Row
import cx.cad.codetests.vendor_factset.Data.fileFor

import scala.collection.parallel.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object VendorData {
  import scala.concurrent.ExecutionContext.Implicits.global

  lazy val geo_data = Future { CsvTools.read(fileFor("mdl__dim_geo.csv")) }
  lazy val vendor_data = Future { CsvTools.read(fileFor("mdl__dim_vendor.csv")) }

  lazy val geosByVendorId = geo_data.map(_.groupBy(_(FieldNames.Vendor.Id)))
  lazy val vendorsByVendorId = vendor_data.map(_.groupBy(_(FieldNames.Vendor.Id)))

  lazy val vendors: immutable.ParIterable[Vendor] = Await.result(vendorsByVendorId, Duration.Inf).par.map { case (vendorId: String, data: Seq[Row]) => {
    val geos: Seq[Row] = Await.result(geosByVendorId, Duration.Inf).getOrElse(vendorId, List.empty)
    print("V")
    Vendor(
      id = VendorId(vendorId),
      vendorData = data.head,
      geoData = geos.toList

    )
  }}
}

