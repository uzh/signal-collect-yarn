package com.signalcollect.util

import com.signalcollect.nodeprovisioning.yarn.YarnClientFactory
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.fs.FileStatus
import java.util.HashMap

class JarUploader(applicationId: String,
    pathToJar: String = ConfigProvider.config.getString("deployment.pathToJar"),
    jarName: String = ConfigProvider.config.getString("deployment.jarName") ) {
  val config = ConfigProvider.config
  val localResources = new HashMap[String, LocalResource]()
  val client = YarnClientFactory.yarnClient
  val fs = FileSystem.get(client.getConfig())
  val src = new Path(pathToJar + jarName)
  val pathSuffix = config.getString("deployment.applicationName") + "/" + applicationId + s"/${jarName}"
  val dst = new Path(fs.getHomeDirectory(), pathSuffix)

  lazy val destStatus = uploadAndGetFileStatus()

  def uploadJar(): HashMap[String, LocalResource] = {

    val jarFile = Records.newRecord(classOf[LocalResource])
    jarFile.setType(LocalResourceType.FILE)
    jarFile.setVisibility(LocalResourceVisibility.PUBLIC)
    jarFile.setResource(ConverterUtils.getYarnUrlFromPath(dst))
    jarFile.setTimestamp(destStatus.getModificationTime())
    jarFile.setSize(destStatus.getLen())
    localResources.put(jarName, jarFile)
    localResources
  }

  private def uploadAndGetFileStatus(): FileStatus = {
    fs.copyFromLocalFile(false, true, src, dst)
    fs.getFileStatus(dst)
  }
}