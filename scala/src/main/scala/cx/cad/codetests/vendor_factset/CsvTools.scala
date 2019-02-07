package cx.cad.codetests.vendor_factset

import java.io.File

import com.github.tototoshi.csv.CSVReader

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object CsvTools {
  type Row = Map[String, String]

  def read(path: File): Seq[Map[String, String]] = {
    println(s"Opening $path")
    val reader = CSVReader.open(path)
    println(s"")
    reader.toStreamWithHeaders
  }
  def cleanRow(row: Row): Row = {
    row.map { case (field: String, value: String) =>
      field -> value.trim()
    }
  }
}

object Data {
  def fileFor(filename: String) = new File(s"../data/${filename}")
}