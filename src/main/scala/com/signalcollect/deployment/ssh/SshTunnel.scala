/*
 *  @author Tobias Bachmann
 *
 *  Copyright 2013 University of Zurich
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
 */

package com.signalcollect.deployment.ssh

import com.jcraft.jsch.JSch
import com.jcraft.jsch.UserInfo
import java.net.Socket
import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException

case class TunnelConfiguration(host: String = "host",
  user: String = "hadoop",
  sshPort: Int = 22,
  ports: List[Int] = List(9000,9022,9023,9024,9025,9026,9035,50070,9200),
  remoteHost: String = "localhost",
  pathToPem: String = "signalcollect.pem")

object SshTunnel {
  def open(config: TunnelConfiguration) {

    val jsch = new JSch()
    jsch.addIdentity(config.pathToPem)
    val session = jsch.getSession(config.user, config.host, config.sshPort)
    session.setConfig("StrictHostKeyChecking", "no")
    try {
      session.connect()
    } catch {
      case e: Throwable => {
        println(e.getLocalizedMessage())
        throw e
      }
    }
    config.ports.foreach(port => session.setPortForwardingL(port, config.remoteHost, port))
  }

  def portIsOpen(ip: String, port: Int, timeout: Int): Boolean = {
    try {
      val socket = new Socket()
      socket.connect(new InetSocketAddress(ip, port), timeout)
      socket.close()
      true
    } catch {
      case e: Throwable => false
    }
  }
  
  def isSshOpen(ip: String, timeout: Int = 5000): Boolean = {
    val sshPort = 22
    portIsOpen(ip, sshPort, timeout)
  }
  
  def getOpenSsh(ips: List[String], timeout: Int = 5000): List[String] = {
    ips.par.filter(ip => isSshOpen(ip, timeout)).toList
  }
  
  def getOneOpenSsh(ips: List[String]): String = {
    getOpenSshRecursive(ips: List[String], 100)
  }
  
  def getOpenSshRecursive(ips: List[String], timeout: Int): String = {
    getOpenSsh(ips, timeout) match {
      case x if timeout > 10000 => throw new TimeoutException
      case Nil => getOpenSshRecursive(ips: List[String], timeout*2)
      case ip :: xs => ip
    }
  }
  
}