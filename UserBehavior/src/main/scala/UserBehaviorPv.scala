
import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
*  TODO 用户访问行为统计
*     统计behavior为pv
*
* */

//  数据样例 543462,1715,1464116,pv,1511658000
case class UserBehavior(
                         useId:Long,
                         itemId:Long,
                         categoryId:Int,
                         behavior:String,
                         timestamp:Long)

object UserBehaviorPv {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    val resourcesPath: URL = getClass.getResource("C:\\Users\\lpc\\Documents\\IDEA_workspace\\UserBehaviorAnalysis\\UserBehavior\\src\\main\\resources\\UserBehavior.csv")
//    println(resourcesPath)
    env
      .readTextFile("C:\\Users\\lpc\\Documents\\IDEA_workspace\\UserBehaviorAnalysis\\UserBehavior\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val linearray: Array[String] = line.split(",")
          UserBehavior(
            linearray(0).toLong,
            linearray(1).toLong,
            linearray(2).toInt,
            linearray(3),
            linearray(4).toLong
          )
      })
        .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior.equals("pv"))
        .timeWindowAll(Time.seconds(60 * 60 ))
        .sum(0)
        .print()

    env.execute("Hot Items Job")
  }
}
