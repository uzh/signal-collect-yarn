/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2014 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.signalcollect.util

import java.io.IOException

import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import org.apache.log4j.LogManager
import org.apache.log4j.net.SocketAppender
import org.apache.log4j.net.SocketNode
import org.apache.log4j.net.SocketServer
import org.apache.log4j.spi.LoggerRepository

trait Logging {
  val loggerName = this.getClass.getName
  lazy val log = Logger.getLogger(loggerName)
}
/**
 * opens a socket, on which remote hosts can connect to send their log messages via SocketAppender
 */
class SocketLogger(port: Int = 4712) extends Thread with Logging {

  override def run() = {
    try {
      val serverSocket = new ServerSocket(port)
      while (true) {
        val socket = serverSocket.accept()
        val inetAddress = socket.getInetAddress()
        val h = LogManager.getLoggerRepository()
        new Thread(new SocketNode(socket, h)).start()
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }
}

/**
 * connects to SocketLogger on host:port to forward log messages.
 */
class LogClient(host: String = "localhost", port: Int = 4712) extends Logging {

  def start {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.DEBUG)

    try {
      val appender = new SocketAppender(host, port)
      appender.setReconnectionDelay(10000)
      rootLogger.addAppender(appender)

    } catch {
      case e: Throwable => println("Failed to add appender !!")
    }
  }
}
