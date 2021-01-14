import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class MovieAnalyticsTestSpec extends FunSuite with BeforeAndAfterEach {

  private val master = "local"

  private val appName = "MovieAnalyticsTestSpec"

  var spark : SparkSession = _

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

}
