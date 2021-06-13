import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.io._

import java.io._
import java.util.Locale
import scala.collection.mutable

object SparkEvents {
  val codecMap = new mutable.HashMap[String, CompressionCodec]

  val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName
  )

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getSparkClassLoader)

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass =
      shortCompressionCodecNames.getOrElse(
        codecName.toLowerCase(Locale.ROOT),
        codecName
      )
    val codec =
      try {
        val ctor = classForName(codecClass).getConstructor(classOf[SparkConf])
        Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
      } catch {
        case _: ClassNotFoundException | _: IllegalArgumentException => None
      }
    codec.getOrElse(
      throw new IllegalArgumentException(
        s"Codec [$codecName] is not available. " +
          s"Consider setting snappy"
      )
    )
  }

  def codecName(log: Path): Option[String] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(".inprogress")
    logName.split("\\.").tail.lastOption
  }

  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      val codec = codecName(log).map { c =>
        codecMap.getOrElseUpdate(c, createCodec(new SparkConf, c))
      }
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  def writeEventsToFile(inputStream: InputStream, outputPath: Path, hadoopConf: Configuration) = {
    val fs = outputPath.getFileSystem(hadoopConf)
    try {
      val buf = new Array[Byte](8192)
      val fos = fs.create(outputPath)
      var len = inputStream.read(buf)
      while (len > 0) {
        fos.write(buf)
        len = inputStream.read(buf)
      }
    } catch {
      case ioe: IOException =>
        println(ioe)
        println("Exiting...")
        System.exit(1)
    } finally {
      inputStream.close()
      fs.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println(
        "Spark event log path is not available. Please try again with the spark event logs path!!"
      )
      return
    }

    val hadoopConf = new Configuration()
    val inputPath = new Path(args(0))
    val inputStream = openEventLog(inputPath, inputPath.getFileSystem(hadoopConf))
    val outputPath = new Path(args(1))
    writeEventsToFile(inputStream, outputPath, hadoopConf)
  }
}
