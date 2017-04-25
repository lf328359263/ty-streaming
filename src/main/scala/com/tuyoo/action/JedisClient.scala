package com.tuyoo.action

import com.tuyoo.config.TYCONFIG
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  *
  */
object JedisClient extends Serializable{

  val redisHost = TYCONFIG.redisHost
  val redisPort = TYCONFIG.redisPort
  val password = TYCONFIG.redisAuth
  val redisTimeout = TYCONFIG.redisTimeOut

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, password)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

}
