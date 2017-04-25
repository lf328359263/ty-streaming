package com.tuyoo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import com.tuyoo.config.TYCONFIG
import redis.clients.jedis.Pipeline
import java.util.Date
import com.tuyoo.action.JedisClient
import org.apache.spark.storage.StorageLevel

/**
  *
  */
object Main {

  def main(args: Array[String]): Unit = {

//    val masterUrl = "local[2]"
    var appName = "gdss_streaming"
    var rollTime = 2
    var topics = "bilog10_2,bilog10_4"
    val zkQuorum = TYCONFIG.zkQuorum
    var groupId = "spark_new_test_1.0"
    var threads = 2
    var maxRate = "4000"
    if (args.length > 0) {
      appName = args(0)
      topics = args(1)
      groupId = args(2)
      threads = args(3).toInt
      rollTime = args(4).toInt
      maxRate = args(5)
    } else {
      println(
        """
      <appName> <topics> <groupId> <threads> <rollTime> <maxRate>
      参数说明：
      0: spark程序名称；
      1: kafka topics（逗号分隔）；
      2：groupId；
      3：每个Topic流启动线程数；
      4：批处理时间（秒）；
      5：单次批处理的行数上限；
      """)
      return
    }

    val allEvent = TYCONFIG.regEvent | TYCONFIG.loginEvent | Set(TYCONFIG.gameNew)
    val topicSet = topics.split(",").toSet
    val topicMap = topicSet.map((_, threads)).toMap
//    val brokers = TYCONFIG.kafkaBrokers
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val conf = new SparkConf().setAppName(appName)
        .set("spark.streaming.receiver.maxRate", maxRate)
//        .setMaster(masterUrl)
    val ssc = new StreamingContext(conf, Seconds(rollTime))
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap, StorageLevel.MEMORY_AND_DISK)
    kafkaStream.map(_._2.split("\t")).filter(fields => {
      fields.length > 20 && allEvent.contains(fields(6))
    })
      .foreachRDD(
        rdd => {
          rdd.foreachPartition(lines => {
            val jedis = JedisClient.pool.getResource
            val jedis2 = JedisClient.pool.getResource
            val activePipeline = jedis.pipelined()
            val designatedPipeline = jedis2.pipelined()
            designatedPipeline.select(40);
            for (line <- lines) {
              analyzeLine(line, activePipeline, designatedPipeline);
//              analyzeLine(line, null, null);
            }
            activePipeline.sync()
            designatedPipeline.sync()
            jedis.close()
            jedis2.close()
          })
        })

    ssc.start()
    ssc.awaitTermination()
  }

  def analyzeLine(line: Array[String], activePipeline: Pipeline, designatedPipeline: Pipeline) = {
    line(2) match {
      case "2" => {
        if (TYCONFIG.loginEvent.contains(line(6))) {
          for (code <- TYCONFIG.loginCode) {
            val key = getKey(line, "6", code, TYCONFIG.loginCodeIndex)
//            println(line(3)+"\t"+key+"\t"+line(7))
            sadd(activePipeline, (line(3)+"000").toLong, key, line(7))
            sadd(designatedPipeline, key, line(7))
          }
        } else if (TYCONFIG.regEvent.contains(line(6))) {
          for (code <- TYCONFIG.loginCode) {
            val key = getKey(line, "1", code, TYCONFIG.loginCodeIndex)
//            println(line(3)+"\t"+key+"\t"+line(7))
            sadd(activePipeline, (line(3)+"000").toLong, key, line(7))
            sadd(designatedPipeline, key, line(7))
          }
        }
      }
      case "4" => {
        if (TYCONFIG.gameNew == line(6)) {
          for (code <- TYCONFIG.gameCode) {
            val key = getKey(line, "2", code, TYCONFIG.gameCodeIndex)
//            println(line(3)+"\t"+key+"\t"+line(7))
            sadd(activePipeline, (line(3)+"000").toLong, key, line(7))
            sadd(designatedPipeline, key, line(7))
          }
        }
      }
    }
  }

  def getKey(line: Array[String], logicType: String, code: String, indexes: Array[Int]) = {
    var k = logicType + ":" + code
    val codeIndexes = code.toCharArray()
    for (i <- 0 until codeIndexes.length) {
      k = if (codeIndexes(i) == '1') k + "_" + line(indexes(i)) else k + "_-1"
    }
    k
  }


  def sadd(pipeline: Pipeline, eventTime: Long, key: String, value: String) {
    val date = new Date(eventTime);
    val db = String.format("%td", date).toInt;
    pipeline.select(db);
    pipeline.sadd(key, value)
    pipeline.expire(key, TYCONFIG.expireTime);
  }


  def sadd(pipeline: Pipeline, key: String, value: String) {
    pipeline.sadd(key, value)
    pipeline.expire(key, TYCONFIG.expireTime);
  }
}
