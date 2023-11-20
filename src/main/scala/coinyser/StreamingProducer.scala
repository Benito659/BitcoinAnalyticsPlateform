package coinyser
import cats.effect.IO
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone
object StreamingProducer  extends StrictLogging{
  def subscribe(onTradeReceived: String => Unit): IO[Unit] = {
    val url = "wss://ws.bitstamp.net"
    val subscribing = "subscribe"
    val currencyChannel = "btcusd"
    val socket = new BitStampWebSocket().subscribeChannel(onTradeReceived,url,subscribing,currencyChannel)
    for {
      _ <- IO(socket.connect())
      _ = Thread.sleep(15000)
      r <- IO(socket.close())

    } yield (r)
  }

//  for {
//      _ <- IO(pusher.connect())
//      channel <- IO(pusher.subscribe("live_trades"))
//      _ <- IO(channel.bind("trade", new SubscriptionEventListener() {
//         override def onEvent(event: PusherEvent): Unit = {
//           val channel = event.getChannelName
//           val eventName = event.getEventName
//           val data = event.getData
//          logger.info(s"Received event: $eventName with data: $data   with channel : $channel")
//          onTradeReceived(data)
//        }
//      }))
//    } yield ()

  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    m.setDateFormat(sdf)
  }

  def deserializeWebsocketTransaction(str: String) = {
    mapper.readValue(str, classOf[Event])
  }

  def convertWsTransaction(wsTx: Event): Transaction =
    Transaction(
      timestamp = new Timestamp(wsTx.data.datetime.toLong*1000), tid =
        wsTx.data.id.toInt, price = wsTx.data.price, sell = wsTx.data.order_type == 1, amount =
        wsTx.data.amount)

  def serializeTransaction(tx: Transaction): String = mapper.writeValueAsString(tx)

}