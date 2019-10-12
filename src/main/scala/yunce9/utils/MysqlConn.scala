package scala.yunce9.utils

import java.sql.Connection

object MysqlConn extends Serializable {

  def connectMysql(): Connection = {
    import java.sql.DriverManager
    //调用Class.forName()方法加载驱动程序
    //    val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&use-SSL=true&useSSL=false&tinyInt1isBit=false"
    val url = "jdbc:mysql://ycmysql.web.zz:3308/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
    try {
      classOf[com.mysql.cj.jdbc.Driver].newInstance()
      DriverManager.getConnection(url, "zzyq", "1qaz2WSX!@")
    }
    catch {
      case ex: Exception => throw ex // Handle other I/O error
    }
  }

  def mysqlConnect(): Connection = {
    import java.sql.DriverManager
    //调用Class.forName()方法加载驱动程序
//    val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
    val url = "jdbc:mysql://ycmysql.web.zz:3308/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
    try {
      classOf[com.mysql.cj.jdbc.Driver].newInstance()
      DriverManager.getConnection(url, "zzyq", "1qaz2WSX!@")
    }
    catch {
      case ex: Exception => throw ex // Handle other I/O error
    }
  }

}
