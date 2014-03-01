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
import org.apache.hadoop.fs.{ FileSystem, Path, FileStatus }
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ Records, ConverterUtils }
import scala.collection.immutable.List
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class YarnDeploymentClient(config: Config) extends LogHelper {

  lazy val yarnClient = YarnClientFactory.getYarnClient(config)
  lazy val application = YarnApplicationFactory.getApplication(config, yarnClient)
  lazy val submissionContext = createSubmissionContext()

  def createSubmissionContext(): ApplicationSubmissionContext = {
    val submissionFactory = new YarnSubmissionContextFactory(yarnClient, config, application)
    submissionFactory.getSubmissionContext
  }

  def submitApplication() {
    log.info("Submitting application to ASM")
    yarnClient.submitApplication(submissionContext)
  }
}