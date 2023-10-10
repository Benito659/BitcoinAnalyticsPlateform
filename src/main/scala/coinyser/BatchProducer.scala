package coinyser
import cats.effect.IO
import cats.implicits._
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.net.URI
object BatchProducer {
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
