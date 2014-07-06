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
package com.signalcollect.deployment.yarn

import com.signalcollect.util.Logging
import com.typesafe.config.Config
import java.net.UnknownHostException
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse

object YarnApplicationCreator extends Logging {

  def getApplication(client: YarnClient): YarnClientApplication = {
    try {
      val app: YarnClientApplication = client.createApplication()
      logResponse(app.getNewApplicationResponse())
      app
    } catch {
      case e: Throwable => {
        println(e.getMessage())
        throw e
      }
    }
  }

  def logResponse(response: GetNewApplicationResponse): Unit = {
    log.info("New application is created: ApplicationId=" + response.getApplicationId() + " MaximumResourceCapability=" + response.getMaximumResourceCapability())
  }
}