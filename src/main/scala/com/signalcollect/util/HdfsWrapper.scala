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
import com.signalcollect.deployment.yarn.YarnClientCreator
import com.signalcollect.deployment.yarn.DefaultYarnClientCreator
import com.signalcollect.deployment.yarn.YarnDeploymentConfiguration

class HdfsWrapper(useDefaultYarnClient: Boolean = false, deploymentConfig: YarnDeploymentConfiguration) {
  val config = ConfigProvider.config
  if(useDefaultYarnClient) YarnClientCreator.useDefaultCreator(deploymentConfig)
  val client = YarnClientCreator.yarnClient
  val fs = FileSystem.get(client.getConfig())

  def uploadFiles(files: List[(String,String)]) {
    files.foreach(file => uploadAndGetFileStatus(file._1,file._2 ))
  }

  def deleteFolder(folder:String) {
    val path = new Path(fs.getHomeDirectory(),folder)
    println(s"deleting folder $path")
    println("deleted: " + fs.delete(path,true))
  }
  
  private def uploadAndGetFileStatus(src: String, dest: String): FileStatus = {
    val srcPath = new Path(src)
    val destPath = new Path(fs.getHomeDirectory(),dest)
    fs.copyFromLocalFile(false, true, srcPath, destPath)
    fs.getFileStatus(destPath)
  }
  
}
