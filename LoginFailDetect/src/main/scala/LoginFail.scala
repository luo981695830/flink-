
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/*
*   基本需求:
*     用户在短时间内频繁登录失败，有程序恶意攻击的可能
*     同一用户(可以是不同IP) 在2秒内连续两次登录失败，需要报警
*   解决思路
*     将用户的登录失败行为存入ListState，设定定时器10秒后触发，
*       查看ListState中有几次失败登录
*     更加精确的检测，可以使用CEP库实现时间流的模式匹配
* */

case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.fromCollection(List(
      LoginEvent(1,"192.168.0.1","fail",1558430842),
      LoginEvent(1,"192.168.0.2","fail",1558430843),
      LoginEvent(1,"192.168.0.3","fail",1558430844),
      LoginEvent(2,"192.168.10,10","success",1558430845)
    ))
        .assignAscendingTimestamps(_.eventTime * 1000)
        .keyBy(_.userId)
        .process(new MatchFunction())
        .print()
    env.execute("Login Fail Detect Job")
  }

  class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent]{
    //定义状态变量
    lazy val loginState:ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("saved login",classOf[LoginEvent]))

    override def processElement(
                                 login: LoginEvent,
                                 context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]
                                   #Context, collector: Collector[LoginEvent]): Unit = {
      if(login.eventType == "fail"){
        loginState.add(login)
      }
      //注册定时器，触发事件设定为2秒后
      context.timerService.registerEventTimeTimer(login.eventTime + 2 * 1000)
    }

    override def onTimer(
                          timestamp: Long,
                          ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]
                            #OnTimerContext, out: Collector[LoginEvent]): Unit = {
      val allLogins:ListBuffer[LoginEvent] = ListBuffer()
      import scala.collection.JavaConversions._
      for(login <- loginState.get){
        allLogins += login
      }
      loginState.clear()

      if(allLogins.length > 1){
        out.collect(allLogins.head)
      }
    }
  }
}
