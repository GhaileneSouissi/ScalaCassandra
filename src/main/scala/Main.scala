import org.apache.spark.sql.SparkSession
import scala.xml.XML
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
/***
  * a main application that start a spark session , connect to a cassandra database, load and xml file and save it in a cassandra database
  */
object main extends App{


    val appConfiguration = ConfigFactory.load()
    val master = appConfiguration.getString("common.spark.master")
    val cassandraUrl = appConfiguration.getString("common.cassandra.url")
    val cassandraFormat = appConfiguration.getString("common.cassandra.format")
    val personPath = appConfiguration.getString("common.xml.persons.path")
    val cityPath = appConfiguration.getString("common.xml.cities.path")



  //set the spark session (a local master with two workers) and create connection with cassandra
  val spark = SparkSession.builder()
    .appName("CassTest")
    .master(master)
    .config("spark.cassandra.connection.host",cassandraUrl)
    .getOrCreate()


  //load users xml file
  val users = XML.loadFile(personPath)
  //load names
  val names = (users \\ "name" ).map(_.text)
  //load ages
  val ages = (users \\ "age" ).map(_.text)

  //build a list of pairs(name:String , age:Int) and transform it to a spark dataframe
  import spark.implicits._
  val person = (names zip ages).map(x=>x._1->x._2.toInt).toDF("username","age")

  //save the dataframe in the cassandra database
  person.write.format(cassandraFormat)
    .mode("append")
    .options(Map( "table" -> "users", "keyspace" -> "users_db"))
    .save()

  //read table users (the dataframe) from cassandra db
  val df = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "users", "keyspace" -> "users_db" ))
    .load()
  //display result
  df.show()

  val villages = XML.loadFile(cityPath)
  val name_villages = (villages \\ "name").map(_.text)
  val city = (villages \\ "city").map(_.text)
  val cities = (name_villages zip city).map(x=>x._1->x._2).toDF("username","city")

  //cities.createCassandraTable("users_db","cities",partitionKeyColumns = Some(Seq("username")))


  cities.write.format(cassandraFormat)
      .mode("append")
    .options(Map( "table" -> "cities", "keyspace" -> "users_db"))
    .save()

  val df1 =  spark
    .read
    .format(cassandraFormat)
    .options(Map( "table" -> "cities", "keyspace" -> "users_db" ))
    .load()
  //display result
  df1.show()
  //join two tables and print the results
  val join = spark.sparkContext.cassandraTable("users_db","users")
    .joinWithCassandraTable("users_db","cities").on(SomeColumns("username"))
  join.foreach(println)


  //join two dataframes
  val users_cities = df.join(df1,df.col("username")===df1.col("username"),"inner").drop(df1.col("username"))
  //create a table in cassandra using that dataframe
  users_cities.createCassandraTable("users_db","users_cities_join",partitionKeyColumns = Some(Seq("username")))

  //write in the cassandra table
  users_cities.write.format(cassandraFormat)
    .mode("append")
    .options(Map( "table" -> "users_cities_join", "keyspace" -> "users_db"))
    .save()

  //read the cassandra table
  val df2 =  spark
    .read
    .format(cassandraFormat)
    .options(Map( "table" -> "users_cities_join", "keyspace" -> "users_db" ))
    .load()
  //display result
  df2.show()


  //stop the session
  spark.stop()


}