package coinyserTest

import coinyser.{BatchProducer, HttpTransaction, Transaction}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import java.sql.Timestamp
class Spec extends FunSuite  with Matchers with BeforeAndAfterAll   {
   private implicit var sparksession: SparkSession = _
  override def beforeAll(): Unit = {
    // Create a SparkSession configuration
    val sparkConf = new SparkConf()
      .setAppName("SparkByExample")
      .setMaster("local[*]") // You can adjust the master URL as needed for your tests

    // Create a SparkSession
    sparksession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

  }

  test("BatchProducer jsonToHttpTransaction create a Dataset[HttpTransaction] from a Json string") {

    val httpTransaction1 = HttpTransaction(
      "1532365695",
      "70683282",
      "7740.00",
      "0",
      "0.10041719"
    )
    val httpTransaction2 = HttpTransaction(
      "1532365693",
      "70683281",
      "7739.99",
      "0",
      "0.00148564"
    )
    val json =
      """[{"date": "1532365695", "tid": "70683282", "price":"7740.00", "type": "0", "amount": "0.10041719"},
        |{"date": "1532365693", "tid": "70683281", "price":"7739.99", "type": "0", "amount":"0.00148564"}]""".stripMargin
    val ds: Dataset[HttpTransaction] = BatchProducer.jsonToHttpTransactions(json)
    ds.collect() should contain theSameElementsAs
      Seq(httpTransaction1, httpTransaction2)
  }

  test(" BatchProducer.httpToDomainTransactions transform a Dataset[HttpTransaction] into a Dataset[Transaction]"){
    val spark = sparksession
    import spark.implicits._
    val httpTransaction1 = HttpTransaction(
      "1532365695",
      "70683282",
      "7740.00",
      "0",
      "0.10041719"
    )
    val httpTransaction2 = HttpTransaction(
      "1532365693",
      "70683281",
      "7739.99",
      "0",
      "0.00148564"
    )
    val source: Dataset[HttpTransaction] = Seq(httpTransaction1,
      httpTransaction2).toDS()
    val target: Dataset[Transaction] =
      BatchProducer.httpToDomainTransactions(source)
    val transaction1 = Transaction(timestamp = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
    val transaction2 = Transaction(timestamp = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)
    target.collect() should contain theSameElementsAs
      Seq(transaction1, transaction2)

  }
  override def afterAll(): Unit = {
    // Stop the SparkSession after all tests are finished
    if (sparksession != null) {
      sparksession.stop()
    }
  }


}
