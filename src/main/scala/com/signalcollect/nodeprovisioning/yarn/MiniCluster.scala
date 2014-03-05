package com.signalcollect.nodeprovisioning.yarn

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler

object MiniCluster {
	lazy val cluster = getCluster()
	def getCluster(): MiniYARNCluster = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64)
    yarnConfig.setClass(YarnConfiguration.RM_SCHEDULER, classOf[FifoScheduler], classOf[ResourceScheduler])
    val cluster = new MiniYARNCluster(classOf[ApplicationMasterSpec].getSimpleName(), 1, 1, 1)
    cluster.init(yarnConfig)
    cluster.start()
    val nodemanager = cluster.getNodeManager(0)
    var attempt: Int = 60;
    val containermanager =
      (nodemanager.getNMContext().getContainerManager()).asInstanceOf[ContainerManagerImpl]
    while (containermanager.getBlockNewContainerRequestsStatus() && attempt > 0) {
      Thread.sleep(2000)
      attempt -= 1
    }
    cluster
  }
}