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
 *
 */
package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import java.net.UnknownHostException
import org.apache.commons.logging.Log
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Records, ConverterUtils}
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class YarnConnector(config: Config) extends LogHelper {

  lazy val yarnConfig = createYarnConfig()
  lazy val yarnClient = createYarnClient()
  lazy val application = createYarnApplication()
  lazy val response = application.getNewApplicationResponse()
  lazy val submissionContext = createSubmissionContext()
  lazy val launchContext = createLaunchContext()

  def createYarnConfig(): Configuration = {
    val yarnOverrides = config.getConfig("deployment.yarn").entrySet().iterator()
    val configToBuild = new YarnConfiguration()
    yarnOverrides.foreach(e => configToBuild.set("yarn." + e.getKey(), e.getValue().unwrapped().toString()))
    configToBuild.reloadConfiguration()
    configToBuild
  }

  def createYarnClient(): YarnClient = {
    val yarnClient = YarnClient.createYarnClient()
    log.info("initialize YarnClient")
    yarnClient.init(yarnConfig)
    yarnClient.start()
    yarnClient
  }

  def createYarnApplication(): YarnClientApplication = {
    try {
      val app: YarnClientApplication = yarnClient.createApplication()
      logResponse(app.getNewApplicationResponse())
      app
    } catch {
      case e: UnknownHostException => {
        log.warn("Couldn't create application: check if ResourceManager is up and running on " +
          config.getString("deployment.yarn.resourcemanager.address"))
        throw e
      }
    }
  }
  
  def createSubmissionContext() : ApplicationSubmissionContext = {
    val context = application.getApplicationSubmissionContext()
    context.setApplicationName(config.getString("deployment.applicationName"))
    context
  }

  def logResponse(response: GetNewApplicationResponse): Unit = {
    log.info("New application is created: ApplicationId=" + response.getApplicationId() + " MaximumResourceCapability=" + response.getMaximumResourceCapability())
  }
  
  def createLaunchContext() = {
    val context = Records.newRecord(classOf[ContainerLaunchContext])
    context.setLocalResources(createLocalResourceForJar())
  }
  
  def createLocalResourceForJar() = {
    val localResources = new HashMap[String, LocalResource]();
    val fs = FileSystem.get(yarnConfig);
    val src = new Path(config.getString("deployment.jar"));
    val pathSuffix = config.getString("deployment.applicationName") + "/" + submissionContext.getApplicationId() + "/AppMaster.jar";	    
    val dst = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, src, dst);
    val destStatus = fs.getFileStatus(dst);
    val amJarRsrc = Records.newRecord(classOf[LocalResource]);
    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);	   
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst)); 
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());
    localResources.put("AppMaster.jar",  amJarRsrc);
    localResources
  }
  
}