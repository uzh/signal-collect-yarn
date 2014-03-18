package com.signalcollect.yarn.deploy

import org.apache.hadoop.util.JarFinder
import java.nio.file.Paths
import com.signalcollect.yarn.applicationmaster.ApplicationMaster

class JarCreator[T](implicit tag: reflect.ClassTag[T]) {
	lazy val jarToAppMaster = JarFinder.getJar(ApplicationMaster.getClass())
	lazy val fullPath = Paths.get(jarToAppMaster)
	lazy val path = fullPath.getParent.toString + "/"
	lazy val fileName = fullPath.getFileName.toString
}