package com.retailpoc.spark

import com.retailpoc.spark.RetailDashboard.{getDataFrameforSheetData, getDataSetSchema}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RetailDashboardTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Online Retail Store")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    val sheetsName = List("Year 2009-2010", "Year 2010-2011")
    val schema = getDataSetSchema()
    val inputFilePath = "data/online_retail_II.xlsx"
    val sampleDF = getDataFrameforSheetData(spark,sheetsName,inputFilePath,schema)
    val rCount = sampleDF.count()
    assert(rCount==1067371, " record count should be 1067371")
  }


}
