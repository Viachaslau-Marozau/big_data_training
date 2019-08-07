package com.epam.courses.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))

  val BIDS_HEADER = StructType(Array("MotelID", "BidDate", "loSa", "Price").map(field => StructField(field, StringType, true)))

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  val ERROR_COLUMN = "HU"
  val BID_DATE_COLUMN = "BidDate"
  val EXC_RATE_DATE_COLUMN = "ValidFrom"
  val MOTEL_ID_COLUMN = "MotelID"
  val EXC_RATE_COLUMN = "ExchangeRate"
  val LOSA_COLUMN = "loSa"
  val PRICE_COLUMN = "Price"
  val MOTEL_NAME_COLUMN = "MotelName"
  val RANK_COLUMN = "Rank"
  val REPORT_ERROR_COLUMN = "Error"
  val REPORT_ERROR_COUNT_COLUMN = "Count"
  val ERROR_PATTERN = "%ERROR%"
  val EMPTY_VALUE = ""
  val WRITE_MODE = "overwrite"
}
