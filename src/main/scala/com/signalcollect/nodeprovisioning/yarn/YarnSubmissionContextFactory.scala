package com.signalcollect.nodeprovisioning.yarn

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import org.apache.hadoop.fs.{ FileSystem, Path, FileStatus }
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.util.{ Records, ConverterUtils }
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._


class YarnSubmissionContextFactory(client: YarnClient, config: Config, application: YarnClientApplication) extends LogHelper {
  private lazy val launchContext: ContainerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])
  val memory = config.getInt("deployment.memory")
  val javaHome =Environment.JAVA_HOME.$()
  val mainClass =  config.getString("deployment.mainClass")
  val jarName = config.getString("deployment.jar")
  val logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR

  def getSubmissionContext(): ApplicationSubmissionContext = {
    val submissionContext = application.getApplicationSubmissionContext()
    setupLaunchAndSubmissionContext(submissionContext)
    submissionContext
  }

  def createLocalResourceForJar(applicationId: String) : HashMap[String, LocalResource] = {
    val filename = config.getString("deployment.jar")
    val localResources = new HashMap[String, LocalResource]()
    val fs = FileSystem.get(client.getConfig())
    val src = new Path(filename)
    val pathSuffix = config.getString("deployment.applicationName") + "/" + applicationId + "/AppMaster.jar"
    val dst = new Path(fs.getHomeDirectory(), pathSuffix)
    
    fs.copyFromLocalFile(false, true, src, dst)
    
    val destStatus = fs.getFileStatus(dst)
    val jarFile = Records.newRecord(classOf[LocalResource])
    jarFile.setType(LocalResourceType.FILE)
    jarFile.setVisibility(LocalResourceVisibility.PUBLIC)
    jarFile.setResource(ConverterUtils.getYarnUrlFromPath(dst))
    jarFile.setTimestamp(destStatus.getModificationTime())
    jarFile.setSize(destStatus.getLen())
    localResources.put(filename, jarFile)
    localResources
  }


  def setupLaunchAndSubmissionContext(submissionContext : ApplicationSubmissionContext) : ApplicationSubmissionContext = {
    setupLaunchContext(submissionContext)
    submissionContext.setApplicationName(config.getString("deployment.applicationName"))
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory)
    submissionContext.setResource(capability)
    submissionContext
  }
  
  private def createCommand: List[String] = {
    
    val command = s"${javaHome}/bin/java -cp $jarName" +
    s" -Xmx${memory}M $mainClass" +
    s" 1> ${logDir}/${jarName}.stdout" +
    s" 2> ${logDir}/${jarName}.stderr"

    log.info("Completed setting up app master command " + command)
    List(command)
  }
  
  private def setupLaunchContext(submissionContext: ApplicationSubmissionContext): Unit = {
    
    val jarResource = createLocalResourceForJar(submissionContext.getApplicationId().toString())
    launchContext.setLocalResources(jarResource)
    
    val commands = createCommand
    launchContext.setCommands(commands)

    submissionContext.setAMContainerSpec(launchContext)
  }
}