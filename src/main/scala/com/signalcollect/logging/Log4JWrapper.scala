package com.signalcollect.logging

import java.net.InetAddress
import org.apache.log4j.Logger

/**
 * contains the global Information about the Current NodeContainer
 */
object NodeContainerInfo{
  var nodeContainerId = -1
  var host = InetAddress.getLocalHost().getHostAddress()
}

/**
 * provides the standard logging methods and adds to the message some MetaInformation about the node
 */
class Log4JWrapper(name: String, nodeContainerId: Int = NodeContainerInfo.nodeContainerId, host: String = NodeContainerInfo.host ) {
  private lazy val log = Logger.getLogger(name)

  private def addNodeInformationToMessage(message: String): String = {
    s"ContainerId:$nodeContainerId Host:$host $message"
   }

  def error(message: String) {
    log.error(addNodeInformationToMessage(message))
  }

  def warn(message: String) {
    log.warn(addNodeInformationToMessage(message))
  }
  
  def info(message: String) {
    log.info(addNodeInformationToMessage(message))
  }
  
  def debug(message: String){
    log.debug(addNodeInformationToMessage(message))
  }
}