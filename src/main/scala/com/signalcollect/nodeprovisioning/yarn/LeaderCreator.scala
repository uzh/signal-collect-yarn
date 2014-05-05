package com.signalcollect.nodeprovisioning.yarn

import com.signalcollect.util.ConfigProvider
import scala.collection.JavaConversions._

object LeaderCreator {
  def getLeader(): NewLeader = {
    val config = ConfigProvider.config
    val baseport = config.getInt("deployment.akka.port")
    val kryoRegistration: List[String] =
      if (config.hasPath("deployment.akka.kryo-registrations"))
        config.getStringList("deployment.akka.kryo-registrations").toList
      else List[String]()
    val kryoInit =
      if (config.hasPath("deployment.akka.kryo-initializer"))
        config.getString("deployment.akka.kryo-initializer")
      else ""
    val numberOfNodes = config.getInt("deployment.numberOfNodes")
    new DefaultLeader(basePort = baseport, numberOfNodes = numberOfNodes, kryoRegistrations = kryoRegistration, kryoInit = kryoInit)
  }
}