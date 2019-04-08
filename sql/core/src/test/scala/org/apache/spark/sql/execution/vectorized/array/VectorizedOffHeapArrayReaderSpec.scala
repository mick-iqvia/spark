package org.apache.spark.sql.execution.vectorized.array

import java.sql.Timestamp

import org.apache.parquet.hadoop.ParquetOutputFormat

// Off heap version of tests, offheap differs only slightly from on-heap and not in type specific way so single type check suffices
class VectorizedOffHeapArrayReaderSpec extends VectorizedArrayReaderSpecBase {
  override def additionalSparkConf: List[(String, String)] = {
    if (SparkSessionProvider._sparkSession != null) SparkSessionProvider._sparkSession.sparkContext.stop()
    List(
      "spark.sql.parquet.enableVectorizedReader" -> "true", // true is default
      "spark.sql.columnVector.offheap.enabled" -> "true", // false is default
      ParquetOutputFormat.DICTIONARY_PAGE_SIZE -> pageSize.toString
    )
  }

  "VectorizedOffHeapArrayReader" should {
    "support timestamps" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => TimestampArray(ints(5).map(i => new Timestamp(i.toLong)))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => TimestampArray(ints().map(i => new Timestamp(i.toLong)))))
      writeToAndReadFromFileAndCompare(randomTimestampArrays())
    }
  }
}
