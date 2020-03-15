


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{TimeCharacteristic, TimerService}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
*    TODO 订单支付实时监控
*   基本需求：
*     用户下单之后，应设置订单失效时间，以提高用户支付的意愿
*       并降低系统风险
*     用户下单后15分钟未支付，则输出监控信息
*
*    解决思路
*     利用CEP库进行时间流的模式匹配，并设定匹配的时间间隔
*
*
* */

case class OrderEvent1(orderId:String,eventType:String,eventTime:String)

class OrderMatchFunction extends KeyedProcessFunction[String,OrderEvent1,OrderEvent1]{
  lazy val orderState:ValueState[OrderEvent1] = getRuntimeContext.getState(
    new ValueStateDescriptor[OrderEvent1]("saved order",classOf[OrderEvent1])
  )

  override def processElement(order: OrderEvent1, context: KeyedProcessFunction[String, OrderEvent1, OrderEvent1]
    #Context, out: Collector[OrderEvent1]): Unit = {
    val timerService = context.timerService

    if(order.eventType == "create"){
      if(orderState.value()== null){
        orderState.update(order)
      }
    }else{
      orderState.update(order)
    }
    timerService.registerEventTimeTimer(
      order.eventTime.toLong * 1000 + 5 * 1000
    )
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, OrderEvent1, OrderEvent1]
                         #OnTimerContext, out: Collector[OrderEvent1]): Unit = {
    val savedOrder = orderState.value()
    if(savedOrder != null && (savedOrder.eventType == "create")){
      out.collect(savedOrder)
    }
    orderState.clear()
  }

}
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orderEventStream = env.fromCollection(List(
      OrderEvent1("1", "create", "1558430842"),
      OrderEvent1("2", "create", "1558430843"),
      OrderEvent1("2", "pay", "1558430844"),
      OrderEvent1("3", "pay", "1558430942"),
      OrderEvent1("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)

   orderEventStream
       .keyBy(_.orderId)
       .process(new OrderMatchFunction)
       .print()

    env.execute("Order Timeout Job")
  }



}
