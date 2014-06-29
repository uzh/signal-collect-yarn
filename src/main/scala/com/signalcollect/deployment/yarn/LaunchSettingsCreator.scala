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

import com.signalcollect.util.ConfigProvider
import java.io.File
import collection.JavaConversions._
import com.signalcollect.deployment.DeploymentConfiguration

object LaunchSettingsCreator {
  def getSettingsForClass(klass: Class[_], deploymentConf: YarnDeploymentConfiguration, testDeployment: Boolean = false): LaunchSettings = {
    val config = ConfigProvider.config
    val createJarOnTheFly = deploymentConf.createJarOnTheFly
    val useMiniCluster = deploymentConf.useMiniCluster
    val memory = deploymentConf.leaderMemory
    val filesToUpload = deploymentConf.copyFiles
    val yarnConfigFiles = if (testDeployment)
      List("yarn.conf", "yarn-testing.conf", "testdeployment.conf")
    else
      List("yarn.conf", "yarn-testing.conf", "deployment.conf")
    val filesOnHdfs = deploymentConf.filesOnHdfs
    val dependencyOnHdfs = deploymentConf.testDependenciesOnHdfs
    if (createJarOnTheFly && useMiniCluster) {
      val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = deploymentConf.testDependencies.split(":").toList.filter(!_.contains("signal-collect-yarn-assembly-1.0-SNAPSHOT") || !dependencyOnHdfs)
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/dummy-yarn-site.xml"
      println(" site xml is" + dummySiteXml)
      val files = pathToJar :: dummySiteXml :: yarnConfigFiles ::: pathToDependencies ::: filesToUpload
      val classpath = (files ::: filesOnHdfs).map(_.split("/").last).mkString(":")
      new LaunchSettings(mainClass = deploymentConf.containerClass,
        memory = memory,
        jvmArguments = deploymentConf.jvmArguments,
        pathsToJars = files,
        filesOnHdfs = filesOnHdfs,
        classpath = classpath)
    } else if (useMiniCluster) {
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/dummy-yarn-site.xml"
      val pathToJar = deploymentConf.pathToJar
      val files = yarnConfigFiles ::: List(dummySiteXml, pathToJar) ::: filesToUpload
      val classpath = files.map(_.split("/").last).mkString(":")
      new LaunchSettings(mainClass = deploymentConf.containerClass,
        memory = memory,
        jvmArguments = deploymentConf.jvmArguments,
        pathsToJars = files,
        filesOnHdfs = filesOnHdfs,
        classpath = classpath)

    } else if (createJarOnTheFly) {
      val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = deploymentConf.testDependencies.split(":").toList.filter(!_.contains("yarn") || !dependencyOnHdfs)
      val files = pathToJar :: yarnConfigFiles ::: pathToDependencies ::: filesToUpload
      val classpath = (files ::: filesOnHdfs).map(_.split("/").last).mkString(":")
      new LaunchSettings(mainClass = deploymentConf.containerClass,
        memory = memory,
        jvmArguments = deploymentConf.jvmArguments,
        pathsToJars = files,
        filesOnHdfs = filesOnHdfs,
        classpath = classpath)
    } else {
      val pathToJar = deploymentConf.pathToJar
      val files = pathToJar :: yarnConfigFiles ::: filesToUpload
      val classpath = files.map(_.split("/").last).mkString(":")
      new LaunchSettings(mainClass = deploymentConf.containerClass,
        memory = memory,
        jvmArguments = deploymentConf.jvmArguments,
        pathsToJars = files,
        filesOnHdfs = filesOnHdfs,
        classpath = classpath)
    }
  }
}