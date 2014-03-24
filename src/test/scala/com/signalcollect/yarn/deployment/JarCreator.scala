package com.signalcollect.yarn.deployment

import com.signalcollect.yarn.utility.JarUtility
import java.io.File
import collection.JavaConversions._
import java.io.IOException
import java.util.jar.JarOutputStream
import java.io.FileOutputStream

object JarCreator {
  
  def createJarFile(classes: Class[_]): String = {
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
    jarFile.getAbsolutePath()
  }
} 

