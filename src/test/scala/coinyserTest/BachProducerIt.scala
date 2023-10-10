package coinyserTest

import cats.effect.unsafe.implicits.global
import coinyser.{BatchProducer, Transaction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp

class BachProducerIt extends FunSuite  with Matchers with BeforeAndAfterAll {
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
  test("BatchProducer.unsafeSave save a Dataset[Transaction] to parquet"){
    val spark = sparksession
    import spark.implicits._
    val tmpDir: File = Files.createTempDirectory("tempDirPrefix").toFile
    try {
      val transaction1 = Transaction(timestamp = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amout = 0.10041719)
      val transaction2 = Transaction(timestamp = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amout = 0.00148564)
      val sourceDS = Seq(transaction1, transaction2).toDS()
      val uri = tmpDir.toURI
      BatchProducer.save(sourceDS, uri).unsafeRunSync()
      tmpDir.list() should contain("date=2018-07-23")
      val readDS = sparksession.read.parquet(uri.toString).as[Transaction]
      sourceDS.collect() should contain theSameElementsAs
        readDS.collect()
    }finally {
      deleteDirectory(tmpDir)
    }
  }

  def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      val files = directory.listFiles()
      if (files != null) {
        for (file <- files) {
          if (file.isDirectory) {
            deleteDirectory(file)
          } else {
            file.delete()
          }
        }
      }
      directory.delete()
    }
  }


  override def afterAll(): Unit = {
    // Stop the SparkSession after all tests are finished
    if (sparksession != null) {
      sparksession.stop()
    }
  }


}
