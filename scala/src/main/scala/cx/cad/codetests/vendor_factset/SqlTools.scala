package cx.cad.codetests.vendor_factset

import java.sql.{Connection, DriverManager, ResultSet}

import org.sqlite.SQLiteException

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object SqlTools {
  type Row = Map[String,String]

  private var _connection: Option[Connection] = None

  val DbFilename = Data.fileFor("db.db")
  def openConnection() = DriverManager.getConnection(s"jdbc:sqlite:$DbFilename")
  def connection = _connection.getOrElse {
    _connection = Option(openConnection())
    _connection.get
  }
  def query(query: String): ResultSet = {
    val statement = connection.createStatement()
    try {
      statement.executeQuery(query)
    } catch {
      case e: SQLiteException => throw new Exception(s"Query was [${query}]", e)
    }
  }

  private def resultSetAsStream(resultSet: ResultSet): Stream[ResultSet] = {
    new Iterator[ResultSet] {
      def hasNext: Boolean = resultSet.next()

      def next(): ResultSet = resultSet
      /*var lastNext = false
      def hasNext = {
        // sqlite doesn't implement ResultSet.isLast
        //!resultSet.isLast
        lastNext
      }
      def next() = {
        lastNext = resultSet.next()
        resultSet
      }*/
    }.toStream
  }

  def resultSetToMap(resultSet: ResultSet): Stream[Row] = {
    val metadata = resultSet.getMetaData
    val columnLabels: Seq[String] = 1.to(metadata.getColumnCount).map { columnIndex => metadata.getColumnLabel(columnIndex) }
    val resultStream = resultSetAsStream(resultSet)

    val data = resultStream.map { record =>
      val columnsAndValues: Map[String, String] = {
        val emptyMap: Map[String, String] = Map.empty
        columnLabels.foldLeft(emptyMap) { case (accumulator: Map[String, String], label: String) =>
          val kv = Map(label -> record.getString(label))
          accumulator ++ kv
        }
      }
      columnsAndValues
    }
    data
  }
}
