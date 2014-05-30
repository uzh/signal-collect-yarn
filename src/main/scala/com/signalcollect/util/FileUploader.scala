/**
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
import com.signalcollect.yarn.deployment.DefaultYarnClientCreator

class FileUploader(applicationId: String, 
  files: List[String] = List(ConfigProvider.config.getString("deployment.pathToJar")), 
  useDefaultYarnClient: Boolean = false) {
  val config = ConfigProvider.config
  val localResources = new HashMap[String, LocalResource]()
  if(useDefaultYarnClient) YarnClientCreator.overrideFactory(new DefaultYarnClientCreator)
  val client = YarnClientCreator.yarnClient
  val fs = FileSystem.get(client.getConfig())

  def uploadFiles(): HashMap[String, LocalResource] = {
    val filesToUpload = getFiles()
    filesToUpload.foreach(jar => {
      val resource = prepareAndUploadFile(jar)
      localResources.put(resource._1, resource._2)
    })
    localResources
  }

  private def getFiles(): List[String] = {
      files
  }

  private def prepareAndUploadFile(srcPath: String): (String, LocalResource) = {
    val jarName = srcPath.split("/").last
    val src = getSource(srcPath,jarName)
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
      config.getString("deployment.hdfspath") + "/" + applicationId + s"/${fileName}"
    
    
  }
  
  private def getSource(jar: String, jarName: String): Path = {
    jarName match {
      case "yarn-site.xml" => new Path(jar.split("/").init.mkString("/") + "/dummy-yarn-site.xml") 
      case _ => new Path(jar)
    }
  }
}