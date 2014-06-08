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

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit

import com.signalcollect.util.ConfigProvider

@RunWith(classOf[JUnitRunner])
class YarnApplicationCreatorSpec extends SpecificationWithJUnit {
  "YarnApplicationCreator" should {
    println("Test executing now: YarnApplicationCreatorSpec")
    val typesafeConfig = ConfigProvider.config
    val yarnClient = YarnClientCreator.yarnClient
    
    "create a new Application" in {
      val application = YarnApplicationCreator.getApplication(typesafeConfig, yarnClient)
      application !== null
      application.getNewApplicationResponse().getApplicationId() !== null
    }

  }
}