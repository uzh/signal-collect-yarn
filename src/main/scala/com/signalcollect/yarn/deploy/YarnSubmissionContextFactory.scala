package com.signalcollect.yarn.deploy

import com.typesafe.config.Config
import com.signalcollect.util.LogHelper
import java.util.HashMap
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.util.Records
import scala.collection.JavaConversions._

class YarnSubmissionContextFactory(client: YarnClient, config: Config, application: YarnClientApplication) extends LogHelper {
  private lazy val submissionContext = application.getApplicationSubmissionContext()
  private lazy val applicationId = submissionContext.getApplicationId().toString()
  private lazy val launchContextFactory = new YarnContainerLaunchContextFactory(config.getString("deployment.applicationMaster"))
  private lazy val launchContext: ContainerLaunchContext =
    launchContextFactory.createLaunchContext(applicationId)
  val memory = config.getInt("deployment.memory")
  
  def getSubmissionContext(): ApplicationSubmissionContext = {
    setupLaunchAndSubmissionContext(submissionContext)
    submissionContext
  }

  def setupLaunchAndSubmissionContext(submissionContext: ApplicationSubmissionContext): ApplicationSubmissionContext = {
    setupLaunchContext(submissionContext)
    submissionContext.setApplicationName(config.getString("deployment.applicationName"))
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory)
    submissionContext.setResource(capability)
    submissionContext
  }

  private def setupLaunchContext(submissionContext: ApplicationSubmissionContext): Unit = {

    submissionContext.setAMContainerSpec(launchContext)
  }
  
}