package org.apache.spark.sql.execution.vectorized.array

import java.sql.{Date, Timestamp}

import org.apache.parquet.hadoop.ParquetOutputFormat

// On heap version of tests
class VectorizedOnHeapArrayReaderSpec extends VectorizedArrayReaderSpecBase {
  override def additionalSparkConf: List[(String, String)] = {
    if (SparkSessionProvider._sparkSession != null) SparkSessionProvider._sparkSession.sparkContext.stop()
    List(
      "spark.sql.parquet.enableVectorizedReader" -> "true", // true is default
      "spark.sql.columnVector.offheap.enabled" -> "false", // false is default
      ParquetOutputFormat.DICTIONARY_PAGE_SIZE -> pageSize.toString
    )
  }

  "VectorizedOnHeapArrayReader" should {
    "support non-null arrays, both dictionary and non-dictionary" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntArray(ints(5))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntArray(ints())))
    }

    "support single element arrays, both dictionary and non-dictionary" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntegerArray(Seq(null.asInstanceOf[Integer]))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntArray(ints(5, 1))))
    }

    "support null arrays" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntArray(null)))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => IntegerArray(null)))
    }

    "support single element empty" in {
      val arrays = (0 until arraySize).map(_ => IntegerArray(Seq(null.asInstanceOf[Integer])))
      writeToAndReadFromFileAndCompare(arrays)
    }

    "support arrays of null" in {
      val arrays = (0 until arraySize).map(i => IntegerArray(nulls(i)))
      writeToAndReadFromFileAndCompare(arrays)
    }

    "support empty arrays" in {
      val arrays = (0 until arraySize).map { i =>
        if (i % 3 == 0) IntArray(Seq())
        else IntArray(ints(i))
      }
      writeToAndReadFromFileAndCompare(arrays)
    }

    "support mixed array entries" in {
      writeToAndReadFromFileAndCompare(randomIntArrays())
    }

    "support longs" in {
      // type specific tests will tests across a limited number of values, which should test dcictionary and
      // also a large number of values which should test non-dictionary
      // also a set of data that mixes things up
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => LongArray(ints(5).map(_.toLong))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => LongArray(ints().map(_.toLong))))
      writeToAndReadFromFileAndCompare(randomLongArrays())
    }

    "support floats" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => FloatArray(ints(5).map(_.toFloat))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => FloatArray(ints().map(_.toFloat))))
      writeToAndReadFromFileAndCompare(randomFloatArrays())
    }

    "support doubles" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => DoubleArray(ints(5).map(_.toDouble))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => DoubleArray(ints().map(_.toDouble))))
      writeToAndReadFromFileAndCompare(randomDoubleArrays())
    }

    "support strings" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => StringArray(ints(5).map(_.toString))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => StringArray(ints().map(_.toString))))
      writeToAndReadFromFileAndCompare(randomStringArrays())
    }

    "support dates" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => DateArray(ints(5).map(i => new Date(i.toLong)))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => DateArray(ints().map(i => new Date(i.toLong)))))
      writeToAndReadFromFileAndCompare(randomDateArrays())
    }

    "support timestamps" in {
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => TimestampArray(ints(5).map(i => new Timestamp(i.toLong)))))
      writeToAndReadFromFileAndCompare((0 until arraySize).map(_ => TimestampArray(ints().map(i => new Timestamp(i.toLong)))))
      writeToAndReadFromFileAndCompare(randomTimestampArrays())
    }

    "still supports non-arrays" in {
      writeToAndReadFromFileAndCompare(randomNonArrays(), arrayType = false)
    }
  }
}
