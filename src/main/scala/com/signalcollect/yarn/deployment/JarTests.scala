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
    jarDir(dir, "", zos)
    jarFile.getAbsolutePath()
  }

  def jarDir(dir: File, relativePath: String, zos: ZipOutputStream) {
    val manifestName = JarFile.MANIFEST_NAME
    val manifestFile = new File(dir, manifestName)
    val manifestEntry = new ZipEntry(manifestName)
    if (!manifestFile.exists) {
      zos.putNextEntry(manifestEntry)
      new Manifest().write(new BufferedOutputStream(zos))
      zos.closeEntry()
    } else {
      val is = new FileInputStream(manifestFile)
      copyToZipStream(is, manifestEntry, zos)
      zos.closeEntry()
      zipDir(dir, relativePath, zos, true);
      zos.close
    }
  }

  def copyToZipStream(is: InputStream, entry: ZipEntry, zos: ZipOutputStream) = {
    zos.putNextEntry(entry)
    val arr = new Array[Byte](4096)
    while (is.read(arr) > -1) {
      val read = is.read(arr)
      zos.write(arr, 0, read)
    }
    is.close()
    zos.closeEntry
  }

  def zipDir(dir: File, relativePath: String, zos: ZipOutputStream, start: Boolean) {
    val dirList = dir.list().toList
    dirList.foreach(traverseDir(_, dir, relativePath, zos, start))
  }

  def traverseDir(currentDir: String, dir: File, relativePath: String, zos: ZipOutputStream, start: Boolean) = {
    val file1 = new File(dir, currentDir)
    if (!file1.isHidden()) {
      if (file1.isDirectory()) {
        if (!start) {
          val dirEntry = new ZipEntry(relativePath + file1.getName() + "/")
          zos.putNextEntry(dirEntry)
          zos.closeEntry()
        }
        val filePath = file1.getPath()
        val file = new File(filePath)
        zipDir(file, relativePath + file1.getName() + "/", zos, false)
      } else {
        val path = relativePath + file1.getName();
        if (!path.equals(JarFile.MANIFEST_NAME)) {
          val anEntry = new ZipEntry(path);
          val is = new FileInputStream(file1);
          copyToZipStream(is, anEntry, zos);
        }
      }
    }
  }
} 

//private static void zipDir(File dir, String relativePath, ZipOutputStream zos,
//                             boolean start) throws IOException {
//    String[] dirList = dir.list();
//    for (String aDirList : dirList) {
//      File f = new File(dir, aDirList);
//      if (!f.isHidden()) {
//        if (f.isDirectory()) {
//          if (!start) {
//            ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
//            zos.putNextEntry(dirEntry);
//            zos.closeEntry();
//          }
//          String filePath = f.getPath();
//          File file = new File(filePath);
//          zipDir(file, relativePath + f.getName() + "/", zos, false);
//        }
//        else {
//          String path = relativePath + f.getName();
//          if (!path.equals(JarFile.MANIFEST_NAME)) {
//            ZipEntry anEntry = new ZipEntry(path);
//            InputStream is = new FileInputStream(f);
//            copyToZipStream(is, anEntry, zos);
//          }
//        }
//      }
//    }
//  }

//private static void copyToZipStream(InputStream is, ZipEntry entry,
//                              ZipOutputStream zos) throws IOException {
//    zos.putNextEntry(entry);
//    byte[] arr = new byte[4096];
//    int read = is.read(arr);
//    while (read > -1) {
//      zos.write(arr, 0, read);
//      read = is.read(arr);
//    }
//    is.close();
//    zos.closeEntry();
//  }
// by JAR spec, if there is a manifest, it must be the first entry in the
    // ZIP.
//    File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
//    ZipEntry manifestEntry = new ZipEntry(JarFile.MANIFEST_NAME);
//    if (!manifestFile.exists()) {
//      zos.putNextEntry(manifestEntry);
//      new Manifest().write(new BufferedOutputStream(zos));
//      zos.closeEntry();
//    } else {
//      InputStream is = new FileInputStream(manifestFile);
//      copyToZipStream(is, manifestEntry, zos);
//    }
//    zos.closeEntry();
//    zipDir(dir, relativePath, zos, true);
//    zos.close();

//Preconditions.checkNotNull(dir, "dir");
//    Preconditions.checkNotNull(jarFile, "jarFile");
//    File jarDir = jarFile.getParentFile();
//    if (!jarDir.exists()) {
//      if (!jarDir.mkdirs()) {
//        throw new IOException(MessageFormat.format("could not create dir [{0}]",
//                                                   jarDir));
//      }
//    }
//    JarOutputStream zos = new JarOutputStream(new FileOutputStream(jarFile));
//    jarDir(dir, "", zos);
//		Preconditions.checkNotNull(klass, "klass");
//		ClassLoader loader = klass.getClassLoader();
//		if (loader != null) {
//			String class_file = klass.getName().replaceAll("\\.", "/")
//					+ ".class";
//			try {
//				for (Enumeration itr = loader.getResources(class_file); itr
//						.hasMoreElements();) {
//					URL url = (URL) itr.nextElement();
//					String path = url.getPath();
//					path = URLDecoder.decode(path, "UTF-8");
//					if ("jar".equals(url.getProtocol())) {
//						path = URLDecoder.decode(path, "UTF-8");
//						return path.replaceAll("!.*$", "");
//					} else if ("file".equals(url.getProtocol())) {
//						String klassName = klass.getName();
//						klassName = klassName.replace(".", "/") + ".class";
//						path = path.substring(0,
//								path.length() - klassName.length());
//						File baseDir = new File(path);
//						File testDir = new File(System.getProperty(
//								"test.build.dir", "target/test-dir"));
//						testDir = testDir.getAbsoluteFile();
//						if (!testDir.exists()) {
//							testDir.mkdirs();
//						}
//						File tempJar = File.createTempFile("hadoop-", "",
//								testDir);
//						tempJar = new File(tempJar.getAbsolutePath() + ".jar");
//						createJar(baseDir, tempJar);
//						return tempJar.getAbsolutePath();
//					}
//				}
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
//}

