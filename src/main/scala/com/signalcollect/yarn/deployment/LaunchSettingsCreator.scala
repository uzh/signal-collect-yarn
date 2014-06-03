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
package com.signalcollect.yarn.deployment

import com.signalcollect.util.ConfigProvider
import java.io.File
import collection.JavaConversions._

object LaunchSettingsCreator {
  def getSettingsForClass(klass: Class[_]): LaunchSettings = {
    val config = ConfigProvider.config
    val createJarOnTheFly = config.getBoolean("deployment.testing.createJarOnTheFly")
    val useMiniCluster = config.getBoolean("deployment.testing.useMiniCluster")
    val memory = config.getInt("deployment.memory")
    val filesToUpload = config.getStringList("deployment.setup.copy-files").toList
    
    if (createJarOnTheFly && useMiniCluster) {
      val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = config.getString("deployment.testing.dependency")
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/dummy-yarn-site.xml"
      println(" site xml is" + dummySiteXml)
      val files = List(pathToJar, pathToDependencies, dummySiteXml) ::: filesToUpload
      new LaunchSettings(pathsToJars = files)
    } else if (useMiniCluster) {
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/dummy-yarn-site.xml"
      val pathToJar = config.getString("deployment.pathToJar")
      val files = List(dummySiteXml, pathToJar) ::: filesToUpload
      new LaunchSettings(pathsToJars = files)

    } else if(createJarOnTheFly) {
       val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = config.getString("deployment.testing.dependency")
      val files = List(pathToJar, pathToDependencies) ::: filesToUpload
      new LaunchSettings(pathsToJars = files)
    } else {
      val pathToJar = config.getString("deployment.pathToJar")
      val files = pathToJar :: filesToUpload
      new LaunchSettings(jvmArguments = config.getString("deployment.jvmArguments"), pathsToJars = files)
    }
  }
}