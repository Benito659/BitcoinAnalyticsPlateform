package coinyserTest


import cats.effect.{Clock, IO, Timer}
import coinyser.{AppContext, BatchProducer, Transaction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

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
      val transaction1 = Transaction(timestamp = new Timestamp(1532365695000L), tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
      val transaction2 = Transaction(timestamp = new Timestamp(1532365693000L), tid = 70683281, price = 7739.99, sell = false, amount = 0.00148564)
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

  test("Batch Producer processOneBatch filter and Save A Batch of transaction , wait 59 min , fetch the next batch"){
    val spark = sparksession
    import spark.implicits._
    val tmpDir: File = Files.createTempDirectory("tempDirPrefix").toFile
    try {
      implicit object FakeTimer extends Timer[IO]{
        private var clockRealTimeInMillis:Long= Instant.parse("2018-08-02T01:00:00Z").toEpochMilli
        def clockRealTime(unit:TimeUnit):IO[Long] = IO(unit.convert(clockRealTimeInMillis,TimeUnit.MILLISECONDS))
        def sleep(duration:FiniteDuration ):IO[Unit] = IO{
          clockRealTimeInMillis=clockRealTimeInMillis+duration.toMillis
          duration.toMillis
        }

        implicit val clock: Clock[IO] = new Clock[IO] {
          def realTime(unit: TimeUnit): IO[Long] = clockRealTime(unit)

          def monotonic(unit: TimeUnit): IO[Long] = ???
        }
      }
      implicit val appContext:AppContext=new AppContext(transactionStorePath = tmpDir.toURI)
      implicit def toTimestamp(str:String):Timestamp=Timestamp.from(Instant.parse(str))
      val tx1= Transaction("2018-08-01T23:00:00Z", 1, 7657.58, true, 0.021762)
      val tx2= Transaction("2018-08-02T01:00:00Z", 2, 7663.85, false,
        0.01385517)
      val tx3= Transaction("2018-08-02T01:58:30Z", 3, 7663.85, false,
        0.03782426)
      val tx4= Transaction("2018-08-02T01:58:59Z", 4, 7663.86, false,
        0.15750809)
      val tx5=Transaction("2018-08-02T02:30:00Z", 5, 7661.49, true, 0.1)

      val txs0 = Seq(tx1)
      val txs1 = Seq(tx2, tx3)
      val txs2 = Seq(tx3, tx4, tx5)
      val txs3 = Seq.empty[Transaction]

      val start0 = Instant.parse("2018-08-02T00:00:00Z")
      val end0 = Instant.parse("2018-08-02T00:59:55Z")
      val threeBatchesIO =
        for {
          tuple1 <- BatchProducer.processOneBatch(IO(txs1.toDS()), txs0.toDS(), start0, end0)
          (ds1, start1, end1) = tuple1
          tuple2 <- BatchProducer.processOneBatch(IO(txs2.toDS()), ds1, start1, end1)
          (ds2, start2, end2) = tuple2
          _ <- BatchProducer.processOneBatch(IO(txs3.toDS()), ds2, start2, end2)
        } yield (ds1, start1, end1, ds2, start2, end2)
      val (ds1, start1, end1, ds2, start2, end2) = threeBatchesIO.unsafeRunSync()
      ds1.collect() should contain theSameElementsAs txs1
      start1 should ===(end0)
      end1 should ===(Instant.parse("2018-08-02T01:58:55Z"))
      ds2.collect() should contain theSameElementsAs txs2
      start2 should ===(end1)
      end2 should ===(Instant.parse("2018-08-02T02:57:55Z"))
      val lastClock = Instant.ofEpochMilli(
        FakeTimer.clockRealTime(TimeUnit.MILLISECONDS).unsafeRunSync())
      lastClock should ===(Instant.parse("2018-08-02T03:57:00Z"))
      val savedTransactions = spark.read.parquet(tmpDir.toString).as[Transaction].collect()
      val expectedTxs = Seq(tx2, tx3, tx4, tx5)
      savedTransactions should contain theSameElementsAs expectedTxs


    } finally {
      deleteDirectory(tmpDir)
    }
  }


  override def afterAll(): Unit = {
    // Stop the SparkSession after all tests are finished
    if (sparksession != null) {
      sparksession.stop()
    }
  }


}
