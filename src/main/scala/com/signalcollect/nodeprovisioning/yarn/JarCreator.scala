package com.signalcollect.nodeprovisioning.yarn

import org.apache.hadoop.util.JarFinder
import java.nio.file.Paths

class JarCreator {
	lazy val jarToAppMaster = JarFinder.getJar(classOf[ApplicationMaster])
	lazy val fullPath = Paths.get(jarToAppMaster)
	lazy val path = fullPath.getParent.toString() + "/"
	lazy val fileName = fullPath.getFileName.toString()
}