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

case class TunnelConfiguration(host: String = "host",
  user: String = "hadoop",
  sshPort: Int = 22,
  localPort: Int = 8088,
  remotePort: Int = 9026,
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
    session.setPortForwardingL(config.localPort, config.remoteHost, config.remotePort)
    while (true) {
      Thread.sleep(1000)
    }
  }

}