package com.signalcollect.yarn.deployment

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream
import java.io.File
import org.apache.hadoop.conf.Configuration
import com.signalcollect.util.JarUploader

object MiniCluster {
  lazy val cluster = startCluster()
  lazy val url = Thread.currentThread().getContextClassLoader().getResource("dummy-yarn-site.xml")

  def startCluster() : MiniYARNCluster = {
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
    println(url)
    if (url == null) {
      throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath")
    }
    val yarnClusterConfig = cluster.getConfig()
    println("Rm in yarnClusterConfig" + yarnClusterConfig.get("yarn.resourcemanager.address"))
//    yarnClusterConfig.set("yarn.application.classpath", (new File(url.getPath())).getParent())

    val bytesOut = new ByteArrayOutputStream()
    yarnClusterConfig.writeXml(bytesOut)
    bytesOut.close()
    val os = new FileOutputStream(new File(url.getPath()))
    os.write(bytesOut.toByteArray())
    os.close()
    
    yarnClusterConfig
  }
}