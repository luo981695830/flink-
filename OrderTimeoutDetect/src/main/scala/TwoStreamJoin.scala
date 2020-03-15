/*
*   实现两条流的join
* */


import org.apache.flink.api.common.state.{ValueState,ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class OrderEvent2(orderId:String,eventType:String,eventTime:String)

case class PayEvent(orderId:String,eventType:String,eventTime:String)

object TwoStreamJoin {
  val unmatchedOrders = new OutputTag[OrderEvent2]("unmatchedOrders"){}
  val unmatchedPays = new OutputTag[PayEvent]("unmatchedPays"){}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orders = env.fromCollection(List(
      OrderEvent2("1", "create", "1558430842"),
      OrderEvent2("2", "create", "1558430843"),
      OrderEvent2("1", "pay", "1558430844"),
      OrderEvent2("2", "pay", "1558430845"),
      OrderEvent2("3", "create", "1558430849"),
      OrderEvent2("3", "pay", "1558430849")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
        .keyBy("orderId")

    val pays = env.fromCollection(List(
      PayEvent("1", "weixin", "1558430847"),
      PayEvent("2", "zhifubao", "1558430848"),
      PayEvent("4", "zhifubao", "1558430850")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy("orderId")

    val processed = orders
      .connect(pays)
      .process(new EnrichmentFunction)

    processed.getSideOutput[PayEvent](unmatchedPays).print()
    processed.getSideOutput[OrderEvent2](unmatchedOrders).print()

    env.execute("Order Timeout Job")
  }

  class EnrichmentFunction extends CoProcessFunction[
    OrderEvent2,PayEvent,(OrderEvent2,PayEvent)]{

    lazy val orderState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent2]("saved order",classOf[OrderEvent2]))

    lazy val payState = getRuntimeContext.getState(new ValueStateDescriptor[PayEvent]("saved pay",classOf[PayEvent]))

    override def processElement1(
                                  order: OrderEvent2,
                                  context: CoProcessFunction[OrderEvent2, PayEvent, (OrderEvent2, PayEvent)]
      #Context, out: Collector[(OrderEvent2, PayEvent)]): Unit = {
        val pay = payState.value()
        if(pay != null){
          payState.clear()
          out.collect((order,pay))
        }else{
          orderState.update(order)
        }
      context.timerService.registerEventTimeTimer(order.eventTime.toLong * 1000)
    }

    override def processElement2(
                                  pay: PayEvent,
                                  context: CoProcessFunction[
                                    OrderEvent2,
                                    PayEvent,
                                    (OrderEvent2, PayEvent)]#Context,
                                  out: Collector[(OrderEvent2, PayEvent)]): Unit = {
        val order = orderState.value()
      if(order != null){
        orderState.clear()
        out.collect((order,pay))
      }else{
        payState.update(pay)
        context.timerService.registerEventTimeTimer(pay.eventTime.toLong * 1000)
      }
    }
    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[
                           OrderEvent2,
                           PayEvent, (OrderEvent2, PayEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent2, PayEvent)]): Unit = {
        if(payState.value != null){
          ctx.output(unmatchedPays,payState.value)
          payState.clear()
        }
        if(orderState.value != null){
          ctx.output(unmatchedOrders,orderState.value)
          orderState.clear()
        }
    }
  }
}