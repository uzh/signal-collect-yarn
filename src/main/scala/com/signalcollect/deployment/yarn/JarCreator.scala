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

import com.signalcollect.yarn.utility.JarUtility
import java.io.File
import collection.JavaConversions._
import java.io.IOException
import java.util.jar.JarOutputStream
import java.io.FileOutputStream
import com.signalcollect.logging.Logging

/**
 * creates a Jar on the fly, it takes all the files in the current project
 */
object JarCreator extends Logging{
  
  def createJarFile(classes: Class[_]): String = {
    log.info("create jar on the fly")
    val classloader = classes.getClassLoader()
    val resources = getResources(classes)
    prepareDirs(resources.get(0), classes)
    
  }
  
  private def prepareDirs(url: java.net.URL, c: Class[_]): String = {
    val path = createPath(c, url.getPath())
    val basedir = createBaseDir(path)
    val testdir = createTestdir()
    createTempJar(testdir, basedir)
  }

  private def getResources(c: Class[_]): List[java.net.URL] = {
    val cl = c.getClassLoader
    val classLocation = c.getName.replaceAll("\\.", "/") + ".class"
    cl.getResources(classLocation).toList
  }

  private def createPath(c: Class[_], path: String): String = {
    val className = c.getName().replace(".", "/") + ".class"
    path.substring(0, path.length - className.length)
  }

  private def createBaseDir(path: String): File = {
    new File(path)
  }

  private def createTestdir(): File = {
    val testDir = (new File(System.getProperty("test.build.dir", "target/test-dir"))).getAbsoluteFile()
    if (!testDir.exists()) testDir.mkdirs()
    testDir
  }

  private def createTempJar(testDir: File, baseDir: File): String = {
    val tempJar1 = File.createTempFile("hadoop-", "", testDir)
    val tempJar = new File(tempJar1.getAbsolutePath() + ".jar")
    createJar(baseDir, tempJar)
  }

  private def createJar(dir: File, jarFile: File): String = {
    val jarDirectory = jarFile.getParentFile()
    if (!jarDirectory.exists())
      if (!jarDirectory.mkdirs())
        throw new IOException("could not create jarDir")
    val zos = new JarOutputStream(new FileOutputStream(jarFile))
    JarUtility.jarDir(dir, "", zos)
    log.info("created jar file")
    jarFile.getAbsolutePath()
  }
} 

