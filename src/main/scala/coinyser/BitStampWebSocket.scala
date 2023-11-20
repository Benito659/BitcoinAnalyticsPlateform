package coinyser

import net.domlom.websocket._
import net.domlom.websocket.model.Websocket

class BitStampWebSocket {
  def subscribeChannel(onTradeReceived: String => Unit,url:String,subscribing:String,currencyChannel:String) : Websocket = {
    val connectionJsonNew =
      s"""
         |{
         |    "event": "bts:$subscribing",
         |    "data": {
         |        "channel": "live_orders_$currencyChannel"
         |    }
         |}
         |""".stripMargin
    val behavior = {
      WebsocketBehavior.debugBehavior
        .setOnOpen(sock => sock.send(connectionJsonNew))
        .setOnMessage { (_, message) =>
          println(s"Message from server: ${message.value}")
          onTradeReceived(message.value)
        }

    }
    Websocket(url, behavior)
  }



}