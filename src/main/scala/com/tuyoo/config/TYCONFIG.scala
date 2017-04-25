package com.tuyoo.config

/**
  *
  */
object TYCONFIG extends Serializable {
  //  kafka配置
  val kafkaBrokers = "HY-10-3-0-51:9092,HY-10-3-0-52:9092,HY-10-3-0-53:9092,HY-10-3-0-54:9092"

  //  zk配置
  val zkQuorum = "HY-10-3-0-49:2181,HY-10-3-0-50:2181,HY-10-3-0-51:2181"

  /**
    * key配置
    */
  val loginCode = Array("10000", "11000", "11100", "11110", "11111")
  val loginCodeIndex = Array(10, 12, 13, 15, 16)
  val gameCode = Array("100000", "110000", "111000", "111110", "111111", "101000")
  val gameCodeIndex = Array(9, 11, 8, 12, 14, 15)

  /**
    * redis配置
    */
  val redisHost = "HY-10-3-0-53"
  val redisPort = 6379
  val redisAuth = "tuyougame"
  val redisTimeOut = 30000
  val expireTime = 2 * 24 * 60 * 60

  /**
    * login事件配置
    */
  val regEvent = Set("11008", "11009", "11010", "11014")
  val loginEvent = Set("11000", "11001", "11002", "11003")

  /**
    * 游戏事件
    */
  val gameStart = "14004"
  val gameEnd = "14006"
  val gameNew = "14001"
}
