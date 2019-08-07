package com.epam.courses.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}
import com.epam.courses.spark.sql.Constants._
import org.apache.spark.sql.expressions.Window

object Runner {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    var sparkConf = new SparkConf()
    sparkConf.setAppName("spark_hw2")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords
      .coalesce(1)
      .write
      .mode(WRITE_MODE)
      .format(CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels, false)
    enriched
      .coalesce(1)
      .write
      .format(CSV_FORMAT)
      .mode(WRITE_MODE)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    sqlContext
      .read
      .load(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    rawBids
      .filter(rawBids(ERROR_COLUMN).like(ERROR_PATTERN))
      .groupBy(rawBids(BID_DATE_COLUMN), rawBids(ERROR_COLUMN).as(REPORT_ERROR_COLUMN))
      .agg(count(rawBids(ERROR_COLUMN)).as(REPORT_ERROR_COUNT_COLUMN))
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    sqlContext
      .read
      .schema(EXCHANGE_RATES_HEADER)
      .format(CSV_FORMAT)
      .load(exchangeRatesPath)
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    val preparedDF = rawBids
      .filter(!rawBids(ERROR_COLUMN).like(ERROR_PATTERN))
      .join(exchangeRates, rawBids(BID_DATE_COLUMN) === exchangeRates(EXC_RATE_DATE_COLUMN))

    val rawBidsUS = selectDataForCountry(preparedDF, TARGET_LOSAS(0))
    val rawBidsCA = selectDataForCountry(preparedDF, TARGET_LOSAS(1))
    val rawBidsMX = selectDataForCountry(preparedDF, TARGET_LOSAS(2))

    rawBidsUS.unionAll(rawBidsCA).unionAll(rawBidsMX).toDF(MOTEL_ID_COLUMN, BID_DATE_COLUMN, LOSA_COLUMN, PRICE_COLUMN)
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    sqlContext
      .read
      .load(motelsPath)
  }

  def getEnriched(bids: DataFrame, motels: DataFrame, removeDublicates: Boolean): DataFrame = {

    val rankBidColumn = getRankColumn(bids, removeDublicates)
    val toDouble: UserDefinedFunction = udf((x: String) => x.toDouble)
    val convertDate: UserDefinedFunction = udf((dateTimeTxt: String) => OUTPUT_DATE_FORMAT.print(INPUT_DATE_FORMAT.parseDateTime(dateTimeTxt)))

    val bidsWithRank = bids
      .join(motels, bids(MOTEL_ID_COLUMN) === motels(MOTEL_ID_COLUMN))
      .select(bids(MOTEL_ID_COLUMN), motels(MOTEL_NAME_COLUMN), convertDate(bids(BID_DATE_COLUMN)), bids(LOSA_COLUMN), toDouble(bids(PRICE_COLUMN)), rankBidColumn)
      .toDF(MOTEL_ID_COLUMN, MOTEL_NAME_COLUMN, BID_DATE_COLUMN, LOSA_COLUMN, PRICE_COLUMN, RANK_COLUMN)

    bidsWithRank
      .where(bidsWithRank(RANK_COLUMN) === 1)
      .select(MOTEL_ID_COLUMN, MOTEL_NAME_COLUMN, BID_DATE_COLUMN, LOSA_COLUMN, PRICE_COLUMN)
  }

  private def selectDataForCountry(rawBids: DataFrame, country: String): DataFrame = {
    rawBids
      .filter(rawBids(country).isNotNull && rawBids(country).notEqual(EMPTY_VALUE))
      .select(
        rawBids(MOTEL_ID_COLUMN),
        rawBids(BID_DATE_COLUMN),
        lit(country),
        round(rawBids(country) * rawBids(EXC_RATE_COLUMN), 3).as(PRICE_COLUMN)
      )
  }

  def getRankColumn(bids: DataFrame, removeDublicates: Boolean): Column = {

    var windowForRank = Window.partitionBy(bids(MOTEL_ID_COLUMN), bids(BID_DATE_COLUMN))

    val loSaPriority = when(bids(LOSA_COLUMN) === TARGET_LOSAS(0), 1)
      .when(bids(LOSA_COLUMN) === TARGET_LOSAS(1), 2)
      .when(bids(LOSA_COLUMN) === TARGET_LOSAS(2), 3)
      .otherwise(4)

    if (removeDublicates) {
      windowForRank = windowForRank.orderBy(desc(PRICE_COLUMN), loSaPriority)
    }
    else {
      windowForRank = windowForRank.orderBy(desc(PRICE_COLUMN))
    }

    rank.over(windowForRank)
  }
}
