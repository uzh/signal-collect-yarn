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

import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.fs.FileStatus
import java.util.HashMap
import com.signalcollect.deployment.yarn.YarnClientCreator
import com.signalcollect.deployment.yarn.DefaultYarnClientCreator

class FileUploader(applicationId: String, 
  files: List[String], 
  useDefaultYarnClient: Boolean = false) {
  println("set username")
  val config = ConfigProvider.config
  val localResources = new HashMap[String, LocalResource]()
  if(useDefaultYarnClient) YarnClientCreator.useDefaultCreator()
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
    println(s"upload file $jarName to $dest")
    uploadFile(jarName, src, dest)
  }
  
   private def prepareAndUploadFile(srcPath: String, destPath: String): (String, LocalResource) = {
    val jarName = srcPath.split("/").last
    val src = getSource(srcPath,jarName)
    val dest = new Path(fs.getHomeDirectory(), destPath)
    println(s"upload file $jarName to $dest")
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
    println(s"uploading file from $src to $dest")
    fs.copyFromLocalFile(false, true, src, dest)
    fs.getFileStatus(dest)
  }
  def createLocalResource(path: String): (String,LocalResource) ={
    val localResource = Records.newRecord(classOf[LocalResource])
    val filePath = new Path(path)
    val fileStatus = fs.getFileStatus(filePath)
    localResource.setType(LocalResourceType.FILE)
    localResource.setVisibility(LocalResourceVisibility.PUBLIC)
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(filePath))
    localResource.setTimestamp(fileStatus.getModificationTime())
    localResource.setSize(fileStatus.getLen())
    (path.split("/").last, localResource)
  }
  
  private def getPathSuffix(fileName: String): String = {
      config.getString("deployment.hdfspath") + "/" + applicationId + s"/${fileName}"
    
    
  }
  
  def getPathOnFs(fileName:String): String = {
    val path =  config.getString("deployment.hdfspath") + "/" + applicationId + s"/${fileName}"
    new Path(fs.getHomeDirectory(), path).toString
  }
  
  private def getSource(jar: String, jarName: String): Path = {
    jarName match {
      case "dummy-yarn-site.xml" => new Path(jar.split("/").init.mkString("/") + "/dummy-yarn-site.xml") 
      case _ => new Path(jar)
    }
  }
}