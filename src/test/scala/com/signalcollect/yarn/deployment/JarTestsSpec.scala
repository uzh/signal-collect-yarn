package com.signalcollect.yarn.deployment

import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationWithJUnit
import com.signalcollect.yarn.applicationmaster.ApplicationMaster
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JarTestsSpec extends SpecificationWithJUnit {
 "JarTests" should {
   "run created jar locally" in {
     val klass = HelloWorld.getClass()
     val pathToJar = JarCreator.createJarFile(klass)
     val fullClassName = klass.getName()
     val className = fullClassName.substring(0, fullClassName.length()-1)
     LocalJarRunner.run(pathToJar, className) must not(throwAn[Exception])
   }
   
   "create jar file out of several classes and run them" in {
     val klass1 = FirstDummy.getClass()
     val klass2 = SecondDummy.getClass()
     val pathToJar = JarCreator.createJarFile(klass1)
     val fullClassName = klass1.getName()
     val className = fullClassName.substring(0, fullClassName.length()-1)
     LocalJarRunner.run(pathToJar, className) must not(throwAn[Exception])
   }
   
 }
}