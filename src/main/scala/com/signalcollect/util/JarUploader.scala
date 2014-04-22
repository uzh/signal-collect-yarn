package com.signalcollect.util

import com.signalcollect.yarn.deployment.YarnClientCreator
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.fs.FileStatus
import java.util.HashMap
import com.signalcollect.yarn.deployment.MiniCluster
import org.apache.hadoop.yarn.client.api.YarnClient

class JarUploader(applicationId: String, 
  jars: List[String] = List(ConfigProvider.config.getString("deployment.pathToJar")),
  client: YarnClient = YarnClientCreator.yarnClient) {
  val config = ConfigProvider.config
  val localResources = new HashMap[String, LocalResource]()
  val fs = FileSystem.get(client.getConfig())

  def uploadJars(): HashMap[String, LocalResource] = {
    val filesToUpload = getFiles()
    filesToUpload.foreach(jar => {
      val resource = prepareAndUploadFile(jar)
      localResources.put(resource._1, resource._2)
    })
    localResources
  }

  private def getFiles(): List[String] = {
      jars
  }

  private def prepareAndUploadFile(jar: String): (String, LocalResource) = {
    val jarName = jar.split("/").last
    val src = getSource(jar,jarName)
    val pathSuffix = getPathSuffix(jarName)
    val dest = new Path(fs.getHomeDirectory(), pathSuffix)
    uploadFile(jarName, src, dest)
  }

  private def uploadFile(jarName: String, src: Path, dest: Path): (String, LocalResource) = {
    val jarFile = Records.newRecord(classOf[LocalResource])
    val destStatus = uploadAndGetFileStatus(src, dest)
    jarFile.setType(LocalResourceType.FILE)
    jarFile.setVisibility(LocalResourceVisibility.PUBLIC)
    jarFile.setResource(ConverterUtils.getYarnUrlFromPath(dest))
    jarFile.setTimestamp(destStatus.getModificationTime())
    jarFile.setSize(destStatus.getLen())
    (jarName, jarFile)
  }

  private def uploadAndGetFileStatus(src: Path, dest: Path): FileStatus = {
    fs.copyFromLocalFile(false, true, src, dest)
    fs.getFileStatus(dest)
  }
  
  private def getPathSuffix(fileName: String): String = {
      config.getString("deployment.applicationName") + "/" + applicationId + s"/${fileName}"
    
    
  }
  
  private def getSource(jar: String, jarName: String): Path = {
    jarName match {
      case "yarn-site.xml" => new Path(jar.split("/").init.mkString("/") + "/dummy-yarn-site.xml") 
      case _ => new Path(jar)
    }
  }
}