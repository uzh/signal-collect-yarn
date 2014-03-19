package com.signalcollect.yarn.deployment

import com.signalcollect.yarn.utility.JarUtility
import java.nio.file.Paths
import com.signalcollect.yarn.applicationmaster.ApplicationMaster

class JarCreator[T](implicit tag: reflect.ClassTag[T]) {
	lazy val jarToAppMaster = JarUtility.getJar(ApplicationMaster.getClass())
	lazy val fullPath = Paths.get(jarToAppMaster)
	lazy val path = fullPath.getParent.toString + "/"
	lazy val fileName = fullPath.getFileName.toString
}