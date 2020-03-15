


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map
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
//输入订单事件数据流
case class OrderEvent(orderId:String,eventType:String,eventTime:String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //读入订单数据
    val orderEventStream = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844"),
      OrderEvent("3", "pay", "1558430942"),
      OrderEvent("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
    //定义一个带时间限制的pattern，选出先创建订单、之后又支付的事件流
    val orderPayPattern= Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .next("next")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))
    //定义一个输出标签，用来表明侧输出流
    val orderTimeOutput = OutputTag[OrderEvent]("orderTimeout")
    // 定义一个输出标签，用来标明侧输出流
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"),orderPayPattern)

    //匿名函数
    val timeoutFunction=(map:Map[String,Iterable[OrderEvent]],timestamp:Long,out:Collector[OrderEvent])=>{
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }
    //匿名函数
    val selectFunction = (map:Map[String,Iterable[OrderEvent]],out:Collector[OrderEvent])=>{}
    //调用flatSelect得到最后的复合输出流
    val timeoutOrder = patternStream.flatSelect(orderTimeOutput)(timeoutFunction)(selectFunction)

    //从复合输出流里拿到侧输出流
    timeoutOrder.getSideOutput(orderTimeOutput).print()
    env.execute("Order Timeout Job")
  }

//  class OrderMatchFunction extends Key

}
