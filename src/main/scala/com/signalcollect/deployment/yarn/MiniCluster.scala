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
package com.signalcollect.deployment.yarn

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler

import com.signalcollect.yarn.applicationmaster.ApplicationMaster
/**
 * this object starts a mini cluster, which is very useful for testing purpose
 */
object MiniCluster {
  val dummyFilename = "dummy-yarn-site.xml"
  lazy val cluster = startCluster()
  lazy val url = Thread.currentThread().getContextClassLoader().getResource(dummyFilename)

  def startCluster(): MiniYARNCluster = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64)
    yarnConfig.setClass(YarnConfiguration.RM_SCHEDULER, classOf[FifoScheduler], classOf[ResourceScheduler])
    val cluster = new MiniYARNCluster(ApplicationMaster.getClass.getSimpleName, 1, 1, 1)
    cluster.init(yarnConfig)
    cluster.start()
    val nodemanager = cluster.getNodeManager(0)
    var attempt: Int = 60
    val containermanager =
      (nodemanager.getNMContext().getContainerManager()).asInstanceOf[ContainerManagerImpl]
    while (containermanager.getBlockNewContainerRequestsStatus() && attempt > 0) {
      Thread.sleep(5000)
      attempt -= 1
    }
    cluster
  }
  
  def getClusterConfig(): Configuration = {
    if (url == null) {
      throw new RuntimeException(s"Could not find ' $dummyFilename ' dummy file in classpath")
    }
    val yarnClusterConfig = cluster.getConfig()
    println("Rm in yarnClusterConfig" + yarnClusterConfig.get("yarn.resourcemanager.address"))

    val bytesOut = new ByteArrayOutputStream()
    yarnClusterConfig.writeXml(bytesOut)
    bytesOut.close()
    val os = new FileOutputStream(new File(url.getPath()))
    os.write(bytesOut.toByteArray())
    os.close()

    yarnClusterConfig
  }
}