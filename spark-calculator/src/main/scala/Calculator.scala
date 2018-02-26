import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Calculator {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: Calculator zk groupId topics numThreads")
      System.exit(1)
    }

    val Array(zk, groupId, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("Calculator").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val topicsMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zk, groupId, topicsMap)

    messages.map(_._2).count().print()
    messages.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}