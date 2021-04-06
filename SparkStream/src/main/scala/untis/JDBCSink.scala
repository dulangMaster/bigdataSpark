package untis

import java.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
  val driver = "com.mysql.jdbc.Driver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _
  var resultSet:java.sql.ResultSet=_

  def open(partitionId: Long, version: Long):Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    val searchname = value.getAs("value").toString

    resultSet = statement.executeQuery("select 1 from spring.test_count where value = '"
      + searchname+"'")
    if(resultSet.next()){
      statement.executeUpdate("update spring.test_count " +
        "set count= '"+ value.getAs("count")+"' where value = '"+searchname+"'")
    } else {
      statement.executeUpdate("INSERT INTO spring.test_count(value,count)" +
        "VALUES ('" + value.getAs("value") + "'," + value.getAs("count") + ");")
    }

  }

  def close(errorOrNull:Throwable):Unit = {
    connection.close
  }
}

