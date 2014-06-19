package com.signalcollect.util

import java.io.FileOutputStream
import java.net.URL
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.nio.channels.Channels
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.compressors.CompressorStreamFactory
import java.io.BufferedInputStream
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import scala.util.Failure
import java.io.InputStream
import scala.util.Success
import scala.util.Try

object FileDownloader {
 
def downloadFile(url: URL, localFileName: String) {
    val in = Channels.newChannel(url.openStream)
    val out = new FileOutputStream(localFileName)
    out.getChannel.transferFrom(in, 0, Int.MaxValue)
    in.close
    out.close
  }

  def decompressGzip(archive: String, decompressedName: String) {
    val zin = new GZIPInputStream(new FileInputStream(archive))
    val os = new FileOutputStream(decompressedName)
    val buffer = new Array[Byte](2048)
    var read = zin.read(buffer)
    while (read > 0) {
      os.write(buffer, 0, read)
      read = zin.read(buffer)
    }
    os.close
    zin.close
  }
  
  def extract(path: String): Unit =  {
 
    def uncompress(input: BufferedInputStream): InputStream =
      Try(new CompressorStreamFactory().createCompressorInputStream(input)) match {
        case Success(i) => new BufferedInputStream(i)
        case Failure(_) => input
      }
 
    def extract(input: InputStream): ArchiveInputStream =
      new ArchiveStreamFactory().createArchiveInputStream(input)
 
 
    val input = extract(uncompress(new BufferedInputStream(new FileInputStream(path))))
    def stream: Stream[ArchiveEntry] = input.getNextEntry match {
      case null  => Stream.empty
      case entry => entry #:: stream
    }
 
    for(entry <- stream if !entry.isDirectory) {
      println(s"${entry.getName} - ${entry.getSize} bytes")
      entry
    }
 
  }
}