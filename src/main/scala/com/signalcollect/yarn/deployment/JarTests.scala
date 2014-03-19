package com.signalcollect.yarn.deployment

import java.io.InputStream
import java.io.BufferedOutputStream
import java.util.jar.JarFile
import java.io.File
import collection.JavaConversions._
import java.net.URLDecoder
import java.io.IOException
import java.util.jar.JarOutputStream
import java.util.jar.Manifest
import java.util.zip.ZipEntry
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.ZipOutputStream
import com.signalcollect.yarn.utility.JarUtility

object JarTests {
  def createJarFile(classes: Class[_]): String = {
    val classloader = classes.getClassLoader()
    val resources = getResources(classes)
    prepareDirs(resources.get(0), classes)
    
  }
  
  def prepareDirs(url: java.net.URL, c: Class[_]): String = {
    val path = createPath(c, url.getPath())
    val basedir = createBaseDir(path)
    val testdir = createTestdir()
    createTempJar(testdir, basedir)
  }

  def getResources(c: Class[_]): List[java.net.URL] = {
    val cl = c.getClassLoader
    val classLocation = c.getName.replaceAll("\\.", "/") + ".class"
    cl.getResources(classLocation).toList
  }

  def createPath(c: Class[_], path: String): String = {
    val className = c.getName().replace(".", "/") + ".class"
    path.substring(0, path.length - className.length)
  }

  def createBaseDir(path: String): File = {
    new File(path)
  }

  def createTestdir(): File = {
    val testDir = new File(System.getProperty("test.build.dir", "target/test-dir")).getAbsoluteFile()
    if (!testDir.exists()) testDir.mkdirs()
    testDir
  }

  def createTempJar(testDir: File, baseDir: File): String = {
    val tempJar1 = File.createTempFile("hadoop-", "", testDir)
    val tempJar = new File(tempJar1.getAbsolutePath() + ".jar")
    createJar(baseDir, tempJar)
  }

  def createJar(dir: File, jarFile: File): String = {
    val jarDirectory = jarFile.getParentFile()
    if (!jarDirectory.exists())
      if (!jarDirectory.mkdirs())
        throw new IOException("could not create jarDir")
    val zos = new JarOutputStream(new FileOutputStream(jarFile))
    JarUtility.jarDir(dir, "", zos)
    jarFile.getAbsolutePath()
  }
} 

