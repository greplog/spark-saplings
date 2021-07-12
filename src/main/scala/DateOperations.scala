import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._


//this code is a rough example of how to create an RDD from underlying database
// and then apply filter, map, then collect collect count of each value

//TODO : this code has compile errors which need to be fixed later on

object DateOperations {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("DateOperations").getOrCreate()
    val sc = spark.sparkContext
    val sampleTable = sc.cassandraTable("my_db", "sample_table")

    val format = "yyyy-MM-dd"
    val dateFormat = new SimpleDateFormat(format)

    sampleTable.filter(row => {
      val booleanColumn = row.get[Option[Boolean]]("boolean_column").getOrElse(false)
      !booleanColumn
    }).map(row => {
      val timestampColumn = row.get[Option[java.util.Date]]("timestamp_column").getOrElse(new Date())
      val dateString = dateFormat.format(timestampColumn)
      dateString
    }).filter(x => x.compareTo("2021-06-03") > 0).countByValue()
  }
}
