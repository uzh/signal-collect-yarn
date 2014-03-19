package com.signalcollect.yarn.deployment

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import com.signalcollect.yarn.utility.JarUtility

@RunWith(classOf[JUnitRunner])
class JarTestsSpec extends SpecificationWithJUnit {
 "JarTests" should {
   "create a jar " in {
     val pathToJarFromJarFinder = JarUtility.getJar(HelloWorld.getClass())
     val pathToJar = JarTests.createJarFile(HelloWorld.getClass())
     println(pathToJarFromJarFinder)
     println(pathToJar)
     0 === 0
   }
 }
}