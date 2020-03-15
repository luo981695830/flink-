import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

object UserBehaviorUv {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env
      .readTextFile("C:\\Users\\lpc\\Documents\\IDEA_workspace\\UserBehaviorAnalysis\\UserBehavior\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val linearray = line.split(",")
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
        .timeWindowAll(Time.seconds(60 * 60))
        .apply(new MyReduceProcessFunction)
        .print()


    env.execute("Hot Items Job")
  }

  class MyReduceProcessFunction extends AllWindowFunction[UserBehavior,Long,TimeWindow]{
    override def apply(window: TimeWindow,
                       vals: Iterable[UserBehavior],
                       out: Collector[Long]): Unit = {
      val s: mutable.Set[Long] = collection.mutable.Set()
      for(v <- vals){
        s += v.itemId
      }
      out.collect(s.size)
    }
  }

}
