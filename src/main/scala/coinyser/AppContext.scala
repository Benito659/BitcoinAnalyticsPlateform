package coinyser

import cats.effect.{IO}
import cats.effect.
import org.apache.spark.sql.SparkSession

import java.net.URI

class AppContext (val transactionStorePath: URI)
                 (implicit val spark: SparkSession,
                  implicit val timer: Timer[IO]){

}
