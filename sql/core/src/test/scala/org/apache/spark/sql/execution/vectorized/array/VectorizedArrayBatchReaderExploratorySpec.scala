package org.apache.spark.sql.execution.vectorized.array

import java.sql.Date

import org.scalatest.{Matchers, WordSpec}

/**
  * Exploratory tests that can be run by hand to examine vectorized reader for arrays
  */
class VectorizedArrayBatchReaderExploratorySpec extends WordSpec with Matchers {
  override def cpuCount: String = "*"
  override def additionalSparkConf: List[(String, String)] = List(
    "spark.sql.parquet.enableVectorizedReader" -> "true", // true is default
    "spark.sql.columnVector.offheap.enabled" -> "false" // false is default
  )

  override def beforeAll() {
    super.beforeAll()
    val person = spark.read.parquet("C:/Users/michael.davies/synpuf/synpuf2300K/parquet/person.parquet")
    person.createOrReplaceTempView("person")
    val drugExposure = spark.read.parquet("C:/Users/michael.davies/synpuf/synpuf2300K/parquet/drug_exposure.parquet")
    drugExposure.createOrReplaceTempView("drug_exposure")

  }

  "ViewSpike" should {
    "array vectorization experiment" taggedAs ByHand in {
      val sql = """SELECT
                  | SUM(size(drugs_drug_concept_id)) AS sum_of_drug_concept_id,
                  | SUM(size(drugs_drug_exposure_start_date)) AS sum_of_drug_exposure_start_date,
                  | SUM(size(drugs_drug_exposure_end_date)) AS sum_of_drug_exposure_end_date,
                  | SUM(size(conditions_condition_concept_id)) AS sum_of_condition_concept_id,
                  | SUM(size(conditions_condition_start_date)) AS sum_of_condition_start_date,
                  | SUM(size(observation_periods_observation_period_start_date)) AS sum_of_observation_period_start_date,
                  | SUM(size(observation_periods_observation_period_end_date)) AS sum_of_observation_period_end_date
                  |FROM person""".stripMargin
      measure("sum_of_array_size non-vectorised", sql)
    }

  }

  private def measure(label: String, sql: String, expected: Option[TTable] = None): Unit = {
    val timer = LoggingMetrics.timer(label)
    // warm up
    spark.sql(sql).show(5)

    (0 until 5).foreach { _ =>
      timer.time {
        expected match {
          case None => spark.sql(sql).show(5)
          case Some(tt) => TTable(spark.sql(sql)) should === (tt)
        }
      }
      LoggingMetrics.reportToLog()
    }
  }
}

case class Person(person_id: Int, drugs_drug_concept_id: Seq[Int], drugs_drug_exposure_start_date: Seq[Date])