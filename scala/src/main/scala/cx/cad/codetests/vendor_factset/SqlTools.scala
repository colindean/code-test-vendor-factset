package cx.cad.codetests.vendor_factset

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * This file is a part of vendor-factset-scala and is licensed as detailed in LICENSE.md.
  */
object SqlTools {

  private var _connection: Option[Connection] = None

  val DbFilename = Data.fileFor("db.db")
  def openConnection() = DriverManager.getConnection(s"jdbc:sqlite:$DbFilename")
  def connection = _connection.getOrElse {
    _connection = Option(openConnection())
    _connection.get
  }
  def query(query: String): ResultSet = {
    val statement = connection.createStatement()
    statement.executeQuery(query)
  }
}
