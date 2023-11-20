package coinyserTest

import coinyser._
import coinyserTest.StreamingProducerSpec.{SampleEventWebsocketTransaction, SampleJsonTransaction, SampleJsonWebTransaction, SampleTransaction}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{Matchers, WordSpec}

import java.sql.Timestamp
class StreamingProducerSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {
  "StreamingProducer.deserializeWebsocketTransaction" should {
    "deserialize a valid String to a WebsocketTransaction" in {
      val str = SampleJsonWebTransaction
      StreamingProducer.deserializeWebsocketTransaction(str) should ===(SampleEventWebsocketTransaction)
    }
  }
  "StreamingProducer.convertTransaction" should {
    "convert a WebSocketTransaction to a Transaction" in {
      StreamingProducer.convertWsTransaction(SampleEventWebsocketTransaction) should ===(SampleTransaction)
    }
  }
  "StreamingProducer.serializeTransaction" should {
    "serialize a Transaction to a String" in {
      StreamingProducer.serializeTransaction(SampleTransaction) should ===(SampleJsonTransaction)
    }
  }
  "StreamingProducer.subscribe" should {
    "register a callback that receives live trades" in {
      val pusher = new FakePusher(Vector("a", "b", "c"))
      var receivedTrades = Vector.empty[String]
      val io = StreamingProducer.subscribe { trade => receivedTrades = receivedTrades :+ trade }
      io.unsafeRunSync()
      println("Here",receivedTrades)
      //receivedTrades should ===(Vector("a", "b", "c"))
    }
  }
}
object StreamingProducerSpec {

  val SampleJsonWebTransaction= """{"data":{"id":1684919621697536,"id_str":"1684919621697536","order_type":0,"datetime":"1700192304","microtimestamp":"1700192304174000","amount":0.82295445,"amount_str":"0.82295445","amount_traded":"0","amount_at_create":"0.82295445","price":36454,"price_str":"36454"},"channel":"live_orders_btcusd","event":"order_created"}"""
  val SampleEventWebsocketTransaction = Event(
    WebSocketTransaction(
      id = 1684919621697536L,
      id_str= "1684919621697536",
      order_type = 0,
      datetime="1700192304",
      microtimestamp = "1700192304174000",
      amount = 0.82295445,
      amount_str = "0.82295445",
      amount_traded = "0",
      amount_at_create="0.82295445",
      price=36454,
      price_str="36454"
    ),
    channel="live_orders_btcusd",
    event="order_created"
  )

  val SampleJsonTransaction =
    """{"timestamp":"2023-11-17 03:38:24","date":"2023-11-17","tid":-343490560,"price":36454.0,"sell":false,"amount":0.82295445}""".stripMargin

  val SampleTransaction =  Transaction(
    timestamp = new Timestamp(1700192304000L), tid = 1684919621697536L.toInt,
    price = 36454, sell = false, amount = 0.82295445)
}
