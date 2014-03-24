package com.signalcollect.yarn.deployment

import com.signalcollect.util.ConfigProvider
import java.io.File

object TestLaunchSettings {
  def getSettingsForClass(klass: Class[_]): LaunchSettings = {
    val config = ConfigProvider.config
    val createJarOnTheFly = config.getBoolean("deployment.testing.createJarOnTheFly")
    val useMiniCluster = config.getBoolean("deployment.testing.useMiniCluster")
    if (createJarOnTheFly && useMiniCluster) {
      val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = config.getString("deployment.testing.dependency")
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/yarn-site.xml"
      val jars = List(pathToJar, pathToDependencies, dummySiteXml)
      new LaunchSettings(pathsToJars = jars)
    } else if (useMiniCluster) {
      val dummySiteXml = new File(MiniCluster.url.getPath).getParent() + "/yarn-site.xml"
      val mainJar = config.getString("deployment.pathToJar")
      new LaunchSettings(pathsToJars = List(dummySiteXml, mainJar))

    } else if(createJarOnTheFly) {
       val pathToJar = JarCreator.createJarFile(klass)
      val pathToDependencies = config.getString("deployment.testing.dependency")
      val jars = List(pathToJar, pathToDependencies)
      new LaunchSettings(pathsToJars = jars)
    } else {
      new LaunchSettings()
    }
  }
}