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

    // 获得日志数据
    val logs = messages.map(_._2)

    // 统计城市活跃度与城市地理位置
    val ips = logs.map(line => {
      line.split("\t")(1)
    })
    ips.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val transport = DataHandler.getTransport()
        val client = DataHandler.getClient(transport)
        partitionRecords.foreach(ip => {
          DataHandler.citySortAndLoca(client, ip)
        })
        DataHandler.destoryTransport(transport)
      })
    })

    // 统计交易总金额
    val payActionUrls = logs.map(line => {
      line.split("\t")(3)
    }).filter(allActionUrl => {
      allActionUrl.substring(0, 4) == "/pay"
    })
    payActionUrls.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val transport = DataHandler.getTransport()
        val client = DataHandler.getClient(transport)
        partitionRecords.foreach(payActionUrl => {
          DataHandler.payGoods(client, payActionUrl)
        })
        DataHandler.destoryTransport(transport)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}