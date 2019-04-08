package org.apache.spark.sql.execution.vectorized.array

import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.scalatest.{Matchers, Suite, WordSpecLike}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random

case class IntArray(array: Seq[Int])
case class IntegerArray(array: Seq[Integer])
case class LongArray(array: Seq[Long])
case class JLongArray(array: Seq[java.lang.Long])
case class FloatArray(array: Seq[Float])
case class JFloatArray(array: Seq[java.lang.Float])
case class DoubleArray(array: Seq[Double])
case class JDoubleArray(array: Seq[java.lang.Double])
case class DateArray(array: Seq[Date])
case class TimestampArray(array: Seq[Timestamp])
case class StringArray(array: Seq[String])
case class NonArray(i: Integer, s: String)

// Supported types are: Int, Long, Float, String, Date (which is stored as Int), DateTime
class VectorizedArrayReaderSpecBase extends WordSpecLike with Matchers {
  this: Suite =>
  protected val pageSize = 2048
  protected val arraySize: Int = 3*pageSize + 1753
  private val r = new Random(1)

  protected def randomIntArrays(): Seq[IntegerArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) IntegerArray(Seq(null))
      else if (i % 7 == 0) IntegerArray(Seq(new Integer(r.nextInt()), null))
      else if (i % 9 == 0) IntegerArray(Seq(null, new Integer(r.nextInt())))
      else if (i % 11 == 0) IntegerArray(null)
      else IntegerArray(integers(r.nextInt()))
    }
    arrays
  }

  protected def randomLongArrays(): Seq[JLongArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) JLongArray(Seq(null))
      else if (i % 7 == 0) JLongArray(Seq(new java.lang.Long(r.nextLong()), null))
      else if (i % 9 == 0) JLongArray(Seq(null, new java.lang.Long(r.nextLong())))
      else if (i % 11 == 0) JLongArray(null)
      else JLongArray(integers(i).map {
        case _: Integer => new java.lang.Long(r.nextLong())
        case _ => null.asInstanceOf[java.lang.Long]
      })
    }
    arrays
  }

  protected def randomFloatArrays(): Seq[JFloatArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) JFloatArray(Seq(null))
      else if (i % 7 == 0) JFloatArray(Seq(new java.lang.Float(r.nextFloat()), null))
      else if (i % 9 == 0) JFloatArray(Seq(null, new java.lang.Float(r.nextFloat())))
      else if (i % 11 == 0) JFloatArray(null)
      else JFloatArray(integers(i).map {
        case _: Integer => new java.lang.Float(r.nextFloat())
        case _ => null.asInstanceOf[java.lang.Float]
      })
    }
    arrays
  }

  protected def randomDoubleArrays(): Seq[JDoubleArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) JDoubleArray(Seq(null))
      else if (i % 7 == 0) JDoubleArray(Seq(new java.lang.Double(r.nextDouble()), null))
      else if (i % 9 == 0) JDoubleArray(Seq(null, new java.lang.Double(r.nextDouble())))
      else if (i % 11 == 0) JDoubleArray(null)
      else JDoubleArray(integers(i).map {
        case _: Integer => new java.lang.Double(r.nextDouble())
        case _ => null.asInstanceOf[java.lang.Double]
      })
    }
    arrays
  }

  protected def randomStringArrays(): Seq[StringArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) StringArray(Seq(null))
      else if (i % 7 == 0) StringArray(Seq(r.nextLong().toString, null))
      else if (i % 9 == 0) StringArray(Seq(null, r.nextLong().toString))
      else if (i % 11 == 0) StringArray(null)
      else StringArray(integers(i).map {
        case _: Integer => r.nextLong().toString
        case _ => null.asInstanceOf[String]
      })
    }
    arrays
  }

  protected def randomDateArrays(): Seq[DateArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) DateArray(Seq(null))
      else if (i % 7 == 0) DateArray(Seq(new Date(r.nextLong()), null))
      else if (i % 9 == 0) DateArray(Seq(null, new Date(r.nextLong())))
      else if (i % 11 == 0) DateArray(null)
      else DateArray(integers(i).map {
        case _: Integer => new Date(r.nextLong())
        case _ => null.asInstanceOf[Date]
      })
    }
    arrays
  }



  protected def randomTimestampArrays(): Seq[TimestampArray] = {
    val arrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) TimestampArray(Seq(null))
      else if (i % 7 == 0) TimestampArray(Seq(new Timestamp(r.nextLong()), null))
      else if (i % 9 == 0) TimestampArray(Seq(null, new Timestamp(r.nextLong())))
      else if (i % 11 == 0) TimestampArray(null)
      else TimestampArray(integers(i).map {
        case _: Integer => new Timestamp(r.nextLong())
        case _ => null.asInstanceOf[Timestamp]
      })
    }
    arrays
  }

  protected def randomNonArrays(): Seq[NonArray] = {
    val nonArrays = (0 until arraySize).map { i =>
      if (i % 5 == 0) NonArray(null, null)
      else if (i % 7 == 0) NonArray(null, i.toString)
      else if (i % 9 == 0) NonArray(i, null)
      else NonArray(i, i.toString)
    }
    nonArrays
  }

  protected def ints(range: Int = Integer.MAX_VALUE, maxSize: Int = 100): Seq[Int] = {
    (0 until r.nextInt(maxSize)).map { _ => r.nextInt(range)}
  }

  protected def integers(start: Int): Seq[Integer] = {
    (0 until r.nextInt(100)).map { i =>
      if (i%(r.nextInt(10) + 1) == 0) null.asInstanceOf[Integer]
      else new Integer(i + start)
    }
  }

  protected def nulls[T](start: Int): Seq[T] = {
    (0 until r.nextInt(100)).map { _ => null.asInstanceOf[T] }
  }

  private val tmpDir = TargetTmpDir.createTmpTargetDir(classOf[VectorizedOnHeapArrayReaderSpec].getSimpleName)
  private var count = 0
  protected def writeToAndReadFromFileAndCompare[T <: Product : ClassTag : TypeTag](arrays: Seq[T], arrayType: Boolean = true): Unit = {
    implicit val encoder: Encoder[T] = Encoders.product[T]
    val df = sc.parallelize(arrays, 1).toDF()
    val path = s"$tmpDir/test$count"
    count += 1
    df.write.parquet(path)
    val df2 = spark.read.parquet(path)
    if (arrayType) assertDataFrameEqualsIgnoreNullness(df, df2)
    else assertDataFrameEquals(df, df2)
  }


  protected def assertDataFrameEqualsIgnoreNullness(expected: DataFrame, result: DataFrame): Unit = {
    def zipWithIndex[U](rdd: RDD[U]) = rdd.zipWithIndex().map{ case (row, idx) => (idx, row) }
    try {
      expected.rdd.cache
      result.rdd.cache
      assert("Length not Equal", expected.rdd.count, result.rdd.count)

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).
        filter {
          case (_, (r1, r2)) => !VectorizedArrayReaderSpecBase.approxArrayEquals(r1, r2, 0.001)
        }

      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }
}

object VectorizedArrayReaderSpecBase extends LazyLogging {
  var rowsOK = 0
  var lengthOK = 0L

  /** Approximate equality which supports arrays, based on equals from [[Row]] */
  def approxArrayEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    if (r1.length != r2.length) {
      return false
    } else {
      (0 until r1.length).foreach(idx => {
        if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
          return false
        }
        if (!r1.isNullAt(idx)) {
          val o1 = r1.getSeq(idx)
          val o2 = r2.getSeq(idx)
          if (o1.size != o2.size) {
            logger.info(s"Failure length for RowsOK: $rowsOK, LengthOK: $lengthOK: $o1 v. $o2")
            return false
          }

          r1.schema.fields(idx).dataType match {
            case ArrayType(IntegerType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case ArrayType(LongType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case ArrayType(FloatType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case ArrayType(DoubleType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case ArrayType(DateType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case ArrayType(TimestampType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                logger.info(s"Failure for RowsOK: $rowsOK, LengthOK: $lengthOK: $o1 v. $o2")
                return false
              }

            case ArrayType(StringType, _) =>
              if (!o1.indices.forall { i => o1(i) == o2(i) }) {
                return false
              }

            case _ =>
              throw new IllegalStateException("Unsupported array comparison")
          }
        }
      })
    }
    rowsOK += 1
    val arrayLength = if (r1.isNullAt(0)) 0L else r1.getSeq(0).size
    lengthOK += arrayLength
    true
  }
}
