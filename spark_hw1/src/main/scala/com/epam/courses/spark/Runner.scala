package com.epam.courses.spark

import com.epam.courses.spark.domain.{BidError, BidItem, EnrichedItem, GroupedBidError}
import com.epam.courses.spark.Constants._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Runner {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("spark_hw1"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[GroupedBidError] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath)
      .map(_.split(DELIMITER))
      .map(_.toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[GroupedBidError] = {
    rawBids.filter(_(ERROR_MESSAGE_INDEX).startsWith(ERROR_MESSAGE_BEGINS))
      .map(list => BidError(list(DATE_INDEX), list(ERROR_MESSAGE_INDEX)))
      .map(errItem => (errItem, 1))
      .reduceByKey{case (x, y) => x + y}
      .map(res => GroupedBidError(res._1.date, res._1.errorMessage, res._2.toString))
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath)
      .map(_.split(DELIMITER))
      .map(list => (list(0), list(3).toDouble))
      .collectAsMap()
      .toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    rawBids.filter(!_ (ERROR_MESSAGE_INDEX).startsWith(ERROR_MESSAGE_BEGINS))
      .flatMap(flatToSeparateLines)
      .map(list => BidItem(
        list(MOTEL_ID_INDEX),
        list(DATE_INDEX),
        list(LOSA_INDEX),
        convertToEuro(list(PRICE_INDEX), exchangeRates(list(DATE_INDEX)))))
      .filter(_.price > 0)
  }

  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath)
      .map(_.split(DELIMITER))
      .map(list => (list(0), list(1)))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    bids.map { bid =>
      (bid.motelId,
        (bid.bidDate,
          bid.loSa,
          bid.price)
      )
    }
      .join(motels)
      .map { case (motelId, (bid, motelName)) =>
        EnrichedItem(
          motelId,
          motelName,
          OUTPUT_DATE_FORMAT.print(INPUT_DATE_FORMAT.parseDateTime(bid._1)),
          bid._2,
          bid._3)
      }
      .map { item =>
        ((item.motelId,
          item.bidDate),
          item)
      }
      .reduceByKey { case (x, y) =>
        if (x.price >= y.price)
          x
        else
          y
      }
      .map(item =>
        item._2
      )
  }


  private def flatToSeparateLines(list: List[String]): List[List[String]] = {
    List(
      List(list(MOTEL_ID_INDEX), list(DATE_INDEX), TARGET_LOSAS(US_LOSA_INDEX), list(US_PRICE_INDEX)),
      List(list(MOTEL_ID_INDEX), list(DATE_INDEX), TARGET_LOSAS(CA_LOSA_INDEX), list(CA_PRICE_INDEX)),
      List(list(MOTEL_ID_INDEX), list(DATE_INDEX), TARGET_LOSAS(MX_LOSA_INDEX), list(MX_PRICE_INDEX))
    )
  }

  private def convertToEuro(strPrice: String, exchangeRate: Double): Double = {
    if (strPrice != null && !strPrice.isEmpty && exchangeRate > 0) {
      EUR_PRICE_FORMAT.format(strPrice.toDouble * exchangeRate).toDouble
    } else {
      -1
    }
  }
}
