

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import scala.collection.Map
/*
*     TODO 恶意登录监控
*   使用CEP 处理恶意登录报警
*   CEP：Complex Event Processing 复杂事件处理库
*     用于在流中筛选符合某种复杂模式的事件
*
* */

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    //定义匹配模式
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)
    // .select方法传入一个pattern select function ,
    // 当检测到定义好的模式序列时就会调用
    val loginFailDataStream = patternStream.select(
      (pattern:Map[String,Iterable[LoginEvent]])=>{
      val first = pattern.getOrElse("begin",null).iterator.next()
      val second = pattern.getOrElse("next",null).iterator.next()
        (second.userId,second.ip,second.eventType)
    })
    //将匹配到的符合条件的事件打印出来
    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")
  }
}
