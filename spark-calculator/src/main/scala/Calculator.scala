import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 作业启动命令：
  * ./spark-submit --master local[5] --class Calculator --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 ../test-lib/spark-calculator-1.0-SNAPSHOT.jar localhost:2181 geekfghuang UduckRec 1
  * ./spark-submit --master local[5] --class Calculator --packages org.apache.spark:spark-streaming-kka-0-8_2.11:2.2.0 ../test-lib/spark-calculator-direct-1.0-SNAPSHOT.jar localhost:9093,localhost:9094 UduckRec
  */
object Calculator {
  def main(args: Array[String]): Unit = {
//    if (args.length != 4) {
//      println("Usage: Calculator zk groupId topics numThreads")
//      System.exit(1)
//    }
//
//    val Array(zk, groupId, topics, numThreads) = args
//    val sparkConf = new SparkConf().setAppName("Calculator")//.setMaster("local[5]")
//    val ssc = new StreamingContext(sparkConf, Seconds(30))
//
//    val topicsMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val messages = KafkaUtils.createStream(ssc, zk, groupId, topicsMap)

    if(args.length != 2) {
      System.err.println("Usage: Calculator <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val sparkConf = new SparkConf().setAppName("Calculator")
      //.setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )

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

    // 统计男女比例
    val userIds = logs.map(line => {
      line.split("\t")(0)
    })
    userIds.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val transport = DataHandler.getTransport()
        val client = DataHandler.getClient(transport)
        partitionRecords.foreach(userId => {
          DataHandler.userSex(client, userId)
        })
        DataHandler.destoryTransport(transport)
      })
    })

    // 统计搜索热词
    val searchActionUrls = logs.map(line => {
      line.split("\t")(3)
    }).filter(searchActionUrl => {
      searchActionUrl.substring(0, 4) == "/sea"
    })
    searchActionUrls.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val transport = DataHandler.getTransport()
        val client = DataHandler.getClient(transport)
        partitionRecords.foreach(searchActionUrl => {
          DataHandler.searchHot(client, searchActionUrl)
        })
        DataHandler.destoryTransport(transport)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}