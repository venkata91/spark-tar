# spark-tar
Extract spark event logs which are compressed with codecs like LZ4, Zstd, snappy etc

## To build
sbt package

## To use
	$SPARK_HOME/bin/spark-submit --master local --class SparkEvents ~/spark-tar_2.12-0.1.jar <input-path (file:///spark-events.lz4)> <output-path (hdfs:///spark-events.log)>
