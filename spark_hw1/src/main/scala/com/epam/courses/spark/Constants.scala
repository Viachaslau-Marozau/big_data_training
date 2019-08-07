package com.epam.courses.spark

import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val BIDS_HEADER = Seq("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq("MotelID", "MotelName", "Country", "URL", "Comment")
  val EXCHANGE_RATES_HEADER = Seq("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

  val TARGET_LOSAS = Seq("US", "CA", "MX")
  val US_LOSA_INDEX = 0
  val CA_LOSA_INDEX = 1
  val MX_LOSA_INDEX = 2

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  val ERROR_MESSAGE_BEGINS = "ERROR_"

  val EUR_PRICE_FORMAT = "%.3f"

  val MOTEL_ID_INDEX = 0
  val DATE_INDEX = 1
  val ERROR_MESSAGE_INDEX = 2
  val US_PRICE_INDEX = 5
  val MX_PRICE_INDEX = 6
  val CA_PRICE_INDEX = 8
  val LOSA_INDEX = 2
  val PRICE_INDEX = 3
}
