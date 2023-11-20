package coinyser

import cats.effect.{IO, Timer}
import org.apache.spark.sql.SparkSession

import java.net.URI

class AppContext (val transactionStorePath: URI)
                 (implicit val spark: SparkSession,
                  implicit val time: Timer[IO]){

}
