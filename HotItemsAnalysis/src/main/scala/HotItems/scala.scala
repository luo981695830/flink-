package HotItems


import java.lang
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/*
*   TODO 实时热门商品统计
*     基本需求：
*       统计近1小时内的热门商品，每5分钟更新一次
*       热门度用浏览次数("pv")来衡量
*
*     解决思路：
*       在所用用户行为数据中，过滤出浏览("pv")行为进行统计
*       构建滑动窗口，窗口长度为1小时，滑动举例为5分钟
*
* */

//创建UserBehavior样例类
case class UserBehavior(useId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)
//创建HotItems样例类
case class HotItems(itemId:Long, windowEnd:Long, count:Long)
// 商品点击量(窗口操作的输出类型)
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)
//Count 统计的聚合函数实现，每出现一条记录就加1
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L
  override def add(in: UserBehavior, acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

//Caused by: java.lang.ClassCastException: org.apache.flink.api.java.tuple.Tuple1 cannot be cast to scala.Tuple1
//用于输出窗口的结果
class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggreagteResult: Iterable[Long],
                     collector: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = aggreagteResult.iterator.next
    collector.collect(ItemViewCount(itemId,window.getEnd,count))
  }
}
//求某个窗口中前N名的热门点击商品，key为窗口时间戳，输出为TopN的结果字符串
class TopHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //命名状态变量的名字和状态变量的类型
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state",classOf[ItemViewCount])
    //定义状态变量
    itemState = getRuntimeContext.getListState(itemStateDesc)

  }

  override def processElement(input: ItemViewCount,
                              context:
                              KeyedProcessFunction[Tuple, ItemViewCount, String]
                                #Context, collector: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(input)
    //注册windowEnd+1的EventTime Timer，当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    //当程序看到windowend + 1的水位线watermark时，触发onTImer回调函数
    context.timerService.registerEventTimeTimer(input.windowEnd+1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]
                         #OnTimerContext, out: Collector[String]): Unit ={
    //获取收到的所有商品点击量
    val allItems:ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get){
      allItems += item
    }
    //提前清楚状态中的数据，释放空键
    itemState.clear()
    // 按照点击量从大到小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //将排名信息格式化成String,便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g. No1: 商品ID = 12224 浏览量=2413
      result
        .append("No")
        .append(i+1)
        .append(":")
        .append(" 商品ID=")
        .append(currentItem.itemId)
        .append(" 浏览量=")
        .append(currentItem.count)
        .append("\n")
    }
    result.append("====================================\n")
    //控制输出频率，模式实时滚动结果
    out.collect(result.toString)
  }
}



object HotItems {
  def main(args: Array[String]): Unit = {
    //创建一个StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设定Time类型为EventTime   Flink默认使用Processing Time处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //为了打印到控制台的结果不乱序，设置全局的并发为1
    env.setParallelism(1)
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers","alyhadoop102:9092")
//    properties.setProperty("group.id","consumer-group")
//    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset","latest")



     env
       //  从本地文件中读取数据
     .readTextFile("C:\\Users\\lpc\\Documents\\IDEA_workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
//    env.
//      addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(
          linearray(0).toLong,
          linearray(1).toLong,
          linearray(2).toInt,
          linearray(3),
          linearray(4).toLong
        )
      })
      //指定时间戳和watermark
      // 这里的数据源经过整理，没有乱序，时间的时间戳是单调递增的
      // 将每条数据的业务时间当作Watermark
      // 得到一个带有时间标记的数据流
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 将点击行为数据过滤出来
      .filter(_.behavior == "pv")
      //按照itemId对商品分组
        .keyBy("itemId")
      //设置活动窗口，统计点击量 窗口大小是一小时 每个5分钟滑动一次
        .timeWindow(Time.minutes(60),Time.minutes(5))
        //做增量的聚合操作
        .aggregate(new CountAgg(),new WindowResultFunction())
         .keyBy("windowEnd")
         .process(new TopHotItems(5))//求点击量前3的商品
         .print()


    env.execute("Hot Items Job")
  }
}





