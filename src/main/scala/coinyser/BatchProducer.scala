package coinyser
import cats.Monad
import cats.effect.{IO, Timer}
import cats.implicits._
import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.net.URI
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
object BatchProducer {
  val WaitTime: FiniteDuration = 59.minute
  val ApiLag: FiniteDuration = 5.seconds

  def filterTxs(transactions: Dataset[Transaction],
                fromInstant: Instant, untilInstant: Instant): Dataset[Transaction] = {
    import transactions.sparkSession.implicits._
    transactions.filter(
      ($"timestamp" >= lit(fromInstant.getEpochSecond).cast(TimestampType)) &&
        ($"timestamp" < lit(untilInstant.getEpochSecond).cast(TimestampType)))
  }

  def currentInstant( implicit timer: Timer[IO]): IO[Instant] = timer.clock.realTime(TimeUnit.SECONDS) map Instant.ofEpochSecond

  def processOneBatch(fetchNextTransactions:IO[Dataset[Transaction]],
                      transactions: Dataset[Transaction],
                      saveStart:Instant,saveEnd:Instant)(implicit appCtx:AppContext):IO[(Dataset[Transaction],Instant,Instant)] = {

    import appCtx._
    val transactionsToSave = filterTxs(transactions, saveStart, saveEnd)
    for {
      _ <- BatchProducer.save(transactionsToSave,
        appCtx.transactionStorePath)
      _ <- IO.sleep(WaitTime)
      beforeRead <- currentInstant
      end = beforeRead.minusSeconds(ApiLag.toSeconds)
      nextTransactions <- fetchNextTransactions
    } yield (nextTransactions, saveEnd, end)

  }

  def truncateInstant(instant: Instant, interval: FiniteDuration): Instant = {
    Instant.ofEpochSecond(instant.getEpochSecond / interval.toSeconds * interval.toSeconds)

  }
  def processRepeatedly(initialJsonTxs: IO[Dataset[Transaction]],
                        jsonTxs: IO[Dataset[Transaction]])
                       (implicit appContext: AppContext): IO[Unit] = {
    import appContext._
    for {
      beforeRead <- currentInstant
      firstEnd = beforeRead.minusSeconds(ApiLag.toSeconds)
      firstTxs <- initialJsonTxs
      firstStart = truncateInstant(firstEnd, 1.day)
      _ <- Monad[IO].tailRecM((firstTxs, firstStart, firstEnd)) {
        case (txs, start, instant) =>
          processOneBatch(jsonTxs, txs, start, instant).map(_.asLeft)
      }
    } yield ()
  }

  def unsafeSave(transactions: Dataset[Transaction], path:URI ):Unit = {
    transactions.write.mode(SaveMode.Append).partitionBy("date").parquet(path.toString)
  }

  def save(transactions: Dataset[Transaction],path:URI):IO[Unit]=IO(unsafeSave(transactions, path))
  def httpToDomainTransactions(ds: Dataset[HttpTransaction]): Dataset[Transaction] = {
    import ds.sparkSession.implicits._
    ds.select(
      $"date".cast(LongType).cast(TimestampType).as("timestamp"),
      $"date".cast(LongType).cast(TimestampType).cast(DateType).as("date"),
      $"tid".cast(IntegerType),
      $"price".cast(DoubleType),
      $"type".cast(BooleanType).as("sell"),
      $"amount".cast(DoubleType)
    ).as[Transaction]

  }

  def jsonToHttpTransactions(json: String)(implicit spark: SparkSession): Dataset[HttpTransaction] = {
    import spark.implicits._
    val ds:Dataset[String]=Seq(json).toDS()
    val txSchema:StructType=Seq.empty[HttpTransaction].toDS().schema
    val schema=ArrayType(txSchema)
    val arrayColumn=from_json(col("value"),schema)
    ds.select(explode(arrayColumn).alias("v"))
      .select("v.*")
      .as[HttpTransaction]

  }

}
