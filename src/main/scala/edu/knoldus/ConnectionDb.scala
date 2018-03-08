package edu.knoldus



import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

import org.apache.log4j.Logger


object ConnectionDb {

  private var instance: ConnectionDb = null
  val JDBC_DRIVER = "com.mysql.jdbc.Driver"
  val DB_URL = "jdbc:mysql://192.168.4.45:3306/TweetDatabase"
  val USER = "root"
  val PASS = "addlife@94"
  var conn: Connection = null

  def getInstance: ConnectionDb = {
    if (instance == null) {
        instance = new ConnectionDb
    }
    instance
  }
}

class ConnectionDb private() {
  val log = Logger.getLogger(this.getClass)
  try
    Class.forName(ConnectionDb.JDBC_DRIVER)
  catch {
    case e: ClassNotFoundException =>
      e.printStackTrace()
  }
  //STEP 3: Open a connection
 log.info("Connecting to database...")

  def getConnection: Connection = {
      if (ConnectionDb.conn == null) try
        ConnectionDb.conn = DriverManager.getConnection(ConnectionDb.DB_URL, ConnectionDb.USER, ConnectionDb.PASS)
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
    ConnectionDb.conn
  }
}
