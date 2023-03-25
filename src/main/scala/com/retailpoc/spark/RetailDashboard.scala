package com.retailpoc.spark

import com.crealytics.spark.excel._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.util.Properties
import scala.io.Source
import scala.language.implicitConversions

object RetailDashboard extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    /* if (args.length == 0) {
    logger.error("Usage: input data filename")
    System.exit(1)
    }*/

    logger.info("Starting execution of Online retail store application ")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val props = getHelperProperties

    val sheetsName = List("Year 2009-2010", "Year 2010-2011") // static sheetName as having hardware constraint to get dynamic sheetName from excel file
    val schema = getDataSetSchema()
    val inputFilePath = "data/online_retail_II.xlsx"
    var currentRootDirectory=System.getProperty("user.dir")

    /**
     * Get the sheetnames by loading data into memory but not recommended for huge files , Method- getAllSheetsName
     */
    //val schema = getAllSheetsName(spark,inputFilePath)

    logger.info("Input excel File Path:" + currentRootDirectory + "/" + inputFilePath)
    val raw_zone_data = getDataFrameforSheetData(spark, sheetsName, inputFilePath, schema).cache()
    if("dev".equalsIgnoreCase(props.getProperty("ENVIRONMENT"))){
      var rawZoneCount = raw_zone_data.count()
      logger.info("rawZoneCount:" + rawZoneCount)
    }
    /**
     * Below snippet can be used to identify the column wise count of null or NaN or any criteria and that will be part of when clause
     */
    //val invalidRecordsDetailsPerColumn = raw_zone_data.select(countCols(raw_zone_data.columns): _*)
    // invalidRecordsDetailsPerColumn.show()

    //val filteredNullValueDF = raw_zone_data.filter(col("Customer ID").isNotNull || col("Description").isNotNull)
    //val invalidRecordsByNullValues = raw_zone_data.filter(col("Customer ID").isNull || col("Description").isNull)
    // Data Validation as per the Atrribute specification
    val stage_zone_data = getValidatedDataFrame(raw_zone_data, props)
    if ("dev".equalsIgnoreCase(props.getProperty("ENVIRONMENT"))) {
      var staggedCount = stage_zone_data.count()
      logger.info("staggedCount:" + staggedCount)
    }
    /**
     * Refined data can be consumed in Analytics purpose.
     * Added meaningful data types to each to get loaded in parquet\csv file format
     */
    val refined_zone_data = getRefinedDataFrame(stage_zone_data)
    if ("dev".equalsIgnoreCase(props.getProperty("ENVIRONMENT"))) {
      var refinedZoneCount = refined_zone_data.count()
      logger.info("refinedZoneCount:" + refinedZoneCount)
    }
    //refined_zone_data.coalesce(1).createOrReplaceTempView("ONLINE_RETAIL_VIEW")// takes 351 sec for job completion
    refined_zone_data.coalesce(1).cache().createOrReplaceTempView("ONLINE_RETAIL_VIEW") //takes 90 sec

    /**
     * Calculate the total revenue for each product (StockCode) by multiplying the Quantity and UnitPrice columns,
     * and then aggregating the results by StockCode.
     */

    val totalRevenueByStockCode = spark.sql(props.getProperty("TOTAL_REVENUE_BY_STOCK_CODE"))
    logger.info("Started Querying to the Table: ONLINE_RETAIL_VIEW with the sql query: "+props.getProperty("TOTAL_REVENUE_BY_STOCK_CODE"))
    saveAsFile(totalRevenueByStockCode,props,"TOTAL_REVENUE_BY_STOCK_CODE")
    logger.info("Queried data has been loaded to output dir")
    /**
     * Find the top 10 most popular products based on the total quantity sold.
     */
    val popularProduct = spark.sql(props.getProperty("POPULAR_PRODUCT"))
    logger.info("Started Querying to the Table: ONLINE_RETAIL_VIEW with the sql query: "+props.getProperty("POPULAR_PRODUCT"))
    saveAsFile(popularProduct,props,"POPULAR_PRODUCT")
    logger.info("Queried data has been loaded to output dir")

    /**
     * Compute the average revenue per product category. Assume that the first three characters of the StockCode represent the product category.
     */
    val productCategory = spark.sql(props.getProperty("AVG_REV_PER_PRODUCT_CATEGORY"))
    logger.info("Started Querying to the Table: ONLINE_RETAIL_VIEW with the sql query: "+props.getProperty("AVG_REV_PER_PRODUCT_CATEGORY"))
    saveAsFile(productCategory,props,"AVG_REV_PER_PRODUCT_CATEGORY")
    logger.info("Queried data has been loaded to output dir")
    /**
     * Create a new column, "InvoiceMonth", which extracts the month and year from the InvoiceDate column (e.g., "2011-12" for "2011-12-08 18:00:00").
     * Calculate the monthly revenue for the entire dataset by aggregating the revenue by InvoiceMonth.
     */
    val monthlyRevenue = spark.sql(props.getProperty("MONTHLY_REVENUE"))
    logger.info("Started Querying to the Table: ONLINE_RETAIL_VIEW with the sql query: "+props.getProperty("MONTHLY_REVENUE"))
    saveAsFile(monthlyRevenue,props,"MONTHLY_REVENUE")
    logger.info("Queried data has been loaded to output dir")

    /**
     * Enable the feature in resource/mapping.properties file to get loaded Invalid records or error records for data analysis
     * By default this feature is disabled.
     */
    if("true".equalsIgnoreCase(props.getProperty("IS_GENERATE_INVALID_RECORDS_FILE"))){
      spark.catalog.dropTempView("ONLINE_RETAIL_VIEW")
      val invalidRecordsByFieldsConstraints = getValidatedDataFrameForInvalidRecords(raw_zone_data, props).coalesce(1).withColumnRenamed("Customer ID","CustomerID")
      saveAsFile(invalidRecordsByFieldsConstraints, props, "Invalidrecords")
      if ("dev".equalsIgnoreCase(props.getProperty("ENVIRONMENT"))) {
        var invalidRecordCount = invalidRecordsByFieldsConstraints.count()
        logger.info("invalidRecordCount:" + invalidRecordCount)
      }
    }
    logger.info("All the jobs has been completed successfully")
    spark.stop()
  }

  /**
   * This method returns consolidated data frame for multiple sheets in excel file
   *
   * @param spark
   * @param sheetsName
   * @param inputFilePath
   * @param datasetSchema
   * @return
   */
  def getDataFrameforSheetData(spark: SparkSession, sheetsName: List[String], inputFilePath: String, datasetSchema: StructType): DataFrame = {
    val firstsheet = sheetsName(0)
    var df = spark.read
      .format("com.crealytics.spark.excel")
      .schema(datasetSchema)
      .option("dataAddress", f"'$firstsheet'!") // Required
      .option("header", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("addColorColumns", "false") // Optional, default: false
      .option("startColumn", 0) // Optional, default: 0
      .option("endColumn", 99) // Optional, default: Int.MaxValue
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .load(inputFilePath)

    for (sheetName <- sheetsName.patch(0, Nil, 1)) {
      val sheetDF = spark.read
        .format("com.crealytics.spark.excel")
        .schema(datasetSchema)
        .option("dataAddress", f"'$sheetName'!") // Required
        .option("header", "true") // Required
        .option("treatEmptyValuesAsNulls", "false") // Optional, default: true - setting up to get all the details
        .option("addColorColumns", "false") // Optional, default: false
        .option("startColumn", 0) // Optional, default: 0
        .option("endColumn", 99) // Optional, default: Int.MaxValue
        .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
        .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
        .load(inputFilePath)

      df = df.union(sheetDF)
    }
    return df
  }

  /**
   * Spark configuratioin by setting up in resources/spark.conf file.
   *
   * @return
   */
  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("resource/spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

  /**
   * InvoiceNo: Invoice number. Nominal. A 6-digit integral number uniquely assigned to each transaction. If this code starts with the letter 'c', it indicates a cancellation.
   * StockCode: Product (item) code. Nominal. A 5-digit integral number uniquely assigned to each distinct product.
   * Description: Product (item) name. Nominal.
   * Quantity: The quantities of each product (item) per transaction. Numeric.
   * InvoiceDate: Invoice date and time. Numeric. The day and time when a transaction was generated.
   * UnitPrice: Unit price. Numeric. Product price per unit in sterling (Â£).
   * CustomerID: Customer number. Nominal. A 5-digit integral number uniquely assigned to each customer.
   * Country: Country name. Nominal. The name of the country where a customer resides.
   *
   * @param filteredNullValueDF
   * @param props
   * @return
   */
  def getValidatedDataFrame(raw_zone_data: Dataset[Row], props: Properties): DataFrame = {
    raw_zone_data.filter((col("Customer ID").isNotNull || col("Description").isNotNull) &&
      ((col("Invoice").cast("int").isNotNull && length(col("Invoice")) === props.getProperty("INVOICE_VALUE_LENGTH")) &&
        (abs(col("Invoice").cast("int")) === col("Invoice").cast("int"))) &&
        ((col("StockCode").cast("int").isNotNull && length(col("StockCode")) === props.getProperty("STOCKCODE_VALUE_LENGTH")) &&
          (abs(col("StockCode").cast("int")) === col("StockCode").cast("int"))) &&
        ((col("Quantity").cast("float").isNotNull) &&
          (abs(col("Quantity").cast("float")) === col("Quantity").cast("Decimal(38,0)"))) &&
        ((col("Price").cast("double").isNotNull) &&
          (abs(col("Price").cast("double")) === col("Price").cast("double"))) &&
        ((col("Customer ID").cast("int").isNotNull && length(col("Customer ID")) === props.getProperty("CUSTOMERID_VALUE_LENGTH")) &&
          (abs(col("Customer ID").cast("int")) === col("Customer ID").cast("int"))) &&
      (col("InvoiceDate").isNotNull)
    )
  }

  /**
   * This method vallidate the data by fields constraints and return invalid data to clean up by taking business decision
   * @param filteredNullValueDF
   * @param props
   * @return
   */
  def getValidatedDataFrameForInvalidRecords(raw_zone_data: Dataset[Row], props: Properties) = {
    raw_zone_data.filter(
      !((col("Customer ID").isNotNull || col("Description").isNotNull) &&
        ((col("Invoice").cast("int").isNotNull && length(col("Invoice")) === props.getProperty("INVOICE_VALUE_LENGTH")) &&
          (abs(col("Invoice").cast("int")) === col("Invoice").cast("int"))) &&
        ((col("StockCode").cast("int").isNotNull && length(col("StockCode")) === props.getProperty("STOCKCODE_VALUE_LENGTH")) &&
          (abs(col("StockCode").cast("int")) === col("StockCode").cast("int"))) &&
        ((col("Quantity").cast("float").isNotNull) &&
          (abs(col("Quantity").cast("float")) === col("Quantity").cast("Decimal(38,0)"))) &&
        ((col("Price").cast("double").isNotNull) &&
          (abs(col("Price").cast("double")) === col("Price").cast("double"))) &&
        ((col("Customer ID").cast("int").isNotNull && length(col("Customer ID")) === props.getProperty("CUSTOMERID_VALUE_LENGTH")) &&
          (abs(col("Customer ID").cast("int")) === col("Customer ID").cast("int"))) &&
        (col("InvoiceDate").isNotNull)))
  }

  /**
   *
   * @param stage_zone_data
   * @return
   */
  def getRefinedDataFrame(stage_zone_data: DataFrame) = {
    stage_zone_data.withColumn("Invoice", col("Invoice").cast("int"))
      .withColumn("StockCode", col("StockCode").cast("int"))
      .withColumn("Quantity", col("Quantity").cast("int"))
      .withColumn("Price", col("Price").cast("double"))
      .withColumn("CustomerID", col("Customer ID").cast("int")).drop("Customer ID")
      .withColumn("InvoiceValue", round(col("Quantity") * col("Price"), 2))
      .withColumn("ProductCategory", substring(col("StockCode"), 0, 3))
      .withColumn("InvoiceMonth", date_format(col("InvoiceDate"), "yyyy-MM"))
  }

  /**
   *
   * @return
   */
  def getDataSetSchema() = StructType(List(
    StructField("Invoice", StringType),
    StructField("StockCode", StringType),
    StructField("Description", StringType),
    StructField("Quantity", StringType),
    StructField("InvoiceDate", TimestampType),
    StructField("Price", StringType),
    StructField("Customer ID", StringType),
    StructField("Country", StringType)
  ))

  /**
   * Get all the keys customized to avoid code change if anything gets change at data level
   *
   * @return
   */
  def getHelperProperties: Properties = {
    val props = new Properties
    props.load(Source.fromFile("resource/mapping.properties").bufferedReader())
    props
  }

  /**
   *
   * @param spark
   * @param inputFilePath
   * @return
   */
  def getAllSheetsName(spark: SparkSession, inputFilePath: String) = {
    org.apache.poi.util.IOUtils.setByteArrayMaxOverride(200000000)
    val wbsss = WorkbookReader(Map("path" -> inputFilePath), spark.sparkContext.hadoopConfiguration)
    wbsss.sheetNames
  }

  /**
   * Utility code:  Snippet to identify null or any pattern by changing condition in when clause in below method
   * val columns = Seq("Invoice","StockCode","Description","Quantity","InvoiceDate","Price","Customer ID","Country")
   *
   * @param columns
   * @return
   */
  def countCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      count(when(col(c).isNull, c)).alias(c)
    })
  }

  /**
   * This is helper method to save Dataframe to save in file with supported file format
   * @param dataFrameToBeSaved
   * @param props
   * @param outPutFileName
   */
  def saveAsFile(dataFrameToBeSaved: DataFrame, props: Properties, outPutFileName: String) = {
    dataFrameToBeSaved.write
      .mode("overwrite")
      .option("header", "true")
      .format(props.getProperty("OUTPUT_FILE_FORMAT"))
      .save(props.getProperty("OUTPUT_SUB_DIRECTORY_PATH") + outPutFileName)
  }

}
