package com.signalcollect.yarn.deployment

import java.io.File
import collection.JavaConversions._

object JarTests {
  def createJarFile(classes: List[Class[_]]): String = {
    val classLoaders =  
    ???
  }
  
  def getResources(c: Class[_]): ??? = {
    val cl = c.getClassLoader
    val classLocation =  c.getName.replaceAll("\\.", "/")
    val urls = cl.getResources(classLocation).toList
    val paths = urls.map(_.getPath)
        
  }
  
}

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