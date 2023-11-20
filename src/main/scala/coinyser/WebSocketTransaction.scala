package coinyser

case class WebSocketTransaction(
                                 id: Long,
                                 id_str: String,
                                 order_type: Int,
                                 datetime:String,
                                 microtimestamp: String,
                                 amount: Double,
                                 amount_str: String,
                                 amount_traded:String,
                                 amount_at_create:String,
                                 price: Double,
                                 price_str: String
                                )
