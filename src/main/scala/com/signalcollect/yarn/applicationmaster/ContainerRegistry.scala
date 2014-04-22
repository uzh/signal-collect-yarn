package com.signalcollect.yarn.applicationmaster

import org.apache.hadoop.yarn.api.records.Container
import com.signalcollect.util.ConfigProvider
import com.signalcollect.nodeprovisioning.yarn.ContainerInfo
import java.net.InetAddress

object ContainerRegistry {

  var containers = Map[String, (Container, Int)]()
  var counter = 0
  var finished = false
  var successfull = true
  var finishedCounter = 0
  var startedCounter = 0

  def register(container: Container): Int = {
    synchronized {
      val containerId = container.getId.toString
      val value = (container, counter)
      containers += ((containerId, value))
      counter += 1
      value._2
    }
  }

  def contains(container: Container): Boolean = {
    synchronized {
      containers.contains(container.getId.toString)
    }
  }

  def remove(container: Container) {
    synchronized {
      containers -= container.getId.toString
    }
  }

  def retrieve(containerId: String): Option[(Container, Int)] = {
    synchronized {
      containers.get(containerId)
    }
  }

  def setSuccessfull(isSuccessfull: Boolean) {
    synchronized {
      successfull = successfull && isSuccessfull
    }
  }

  def setFinished(numberOfFinishedContainers: Int) {
    synchronized {
      finishedCounter += numberOfFinishedContainers
    }
  }

  def isFinished: Boolean = {
    synchronized {
      finishedCounter == ConfigProvider.config.getInt("deployment.numberOfNodes")
    }
  }

  def containerStarted() {
    synchronized {
      startedCounter += 1
    }
  }
  
  def allStarted: Boolean = {
    synchronized {
      startedCounter == ConfigProvider.config.getInt("deployment.numberOfNodes")
    }
  }
  
  def failContainers() {
    synchronized {
      successfull = false
      finished = true
    }
  }
  
  def getContainerNodes(): List[ContainerInfo] = {
    
    val akkaPort = ConfigProvider.config.getInt("deployment.akka.port")
    val containerNodes = containers.values.map(t => new ContainerInfo(getIpFromHostAddress(t._1), t._2, akkaPort)).toList
    containerNodes
  }
  
  def getIpFromHostAddress(container: Container): String = {
    InetAddress.getByName(container.getNodeHttpAddress().split(":").head).getHostAddress()
  }
}