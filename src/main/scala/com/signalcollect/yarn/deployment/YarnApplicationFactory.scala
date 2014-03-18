package com.signalcollect.yarn.deployment

import com.signalcollect.util.LogHelper
import com.typesafe.config.Config
import java.net.UnknownHostException
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse

object YarnApplicationFactory extends LogHelper {

  def getApplication(config: Config, client: YarnClient): YarnClientApplication = {
    try {
      val app: YarnClientApplication = client.createApplication()
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

  def logResponse(response: GetNewApplicationResponse): Unit = {
    log.info("New application is created: ApplicationId=" + response.getApplicationId() + " MaximumResourceCapability=" + response.getMaximumResourceCapability())
  }
}