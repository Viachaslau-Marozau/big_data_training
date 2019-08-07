package com.epam.courses.spark

import java.io.File
import java.nio.file.Files

import com.epam.courses.spark.Runner.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.courses.spark.domain.{BidError, BidItem, EnrichedItem, GroupedBidError}
import com.epam.courses.spark.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class RunnerTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"
  val INPUT_BIDS_SAMPLE_NO_PRICES = "src/test/resources/bids_sample_no_prices.txt"
  val INPUT_EXCHANGE_RATES_SIX_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTEL_SAMPLE = "src/test/resources/motels_sample.txt"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = Runner.getRawBids(sc, INPUT_BIDS_SAMPLE)
    assertRDDEquals(rawBids, expected)
  }

  test("should read raw bids without price") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "11-05-08-2016"),
        List("0000002", "11-05-08-2017"),
        List("0000002", "11-05-08-2018")
      )
    )

    val rawBids = Runner.getRawBids(sc, INPUT_BIDS_SAMPLE_NO_PRICES)
    assertRDDEquals(rawBids, expected)
  }

  test("should read error records and calculate count") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("1", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected: RDD[GroupedBidError] = sc.parallelize(
      Seq(
        GroupedBidError("06-05-02-2016", "ERROR_1", "2"),
        GroupedBidError("06-05-02-2016", "ERROR_2", "1"),
        GroupedBidError("07-05-02-2016", "ERROR_2", "1")
      )
    )

    val erroneousRecords = Runner.getErroneousRecords(rawBids)
    assertRDDEquals(expected, erroneousRecords)
  }

  test("should create corrrect bids") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0", "0", "0", "0.5", "1.1", "0", "0.9", "0"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val exchangeRates =
      Map(
        ("15-04-08-2016", 0.5),
        ("15-04-08-2017", 0.2),
        ("15-04-08-2017", 0.75)

      )

    val expected = sc.parallelize(
      Seq(
        new BidItem("2", "15-04-08-2016", "US", 0.25),
        new BidItem("2", "15-04-08-2016", "CA", 0.45),
        new BidItem("2", "15-04-08-2016", "MX", 0.55)
      )
    )

    val bids = Runner.getBids(rawBids, exchangeRates)
    assertRDDEquals(expected, bids)
  }

  test("should read exchange rates") {
    val expected =
      Map(
        ("11-06-05-2016", 0.803),
        ("11-05-08-2016", 0.873),
        ("10-06-11-2015", 0.987),
        ("10-05-02-2016", 0.876),
        ("09-05-02-2016", 0.948),
        ("10-06-05-2016", 0.848)
      )

    val exchangeRates = Runner.getExchangeRates(sc, INPUT_EXCHANGE_RATES_SIX_SAMPLE)
    Assert.assertTrue(exchangeRates.size == 6)
    Assert.assertEquals(exchangeRates, expected)
  }

  test("should read motels") {
    val expected = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino")
      )
    )

    val motels = Runner.getMotels(sc, INPUT_MOTEL_SAMPLE)
    assertRDDEquals(motels, expected)
  }

  test("should get bid with max price") {
    val motels = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn")
      )
    )

    val bids = sc.parallelize(
      Seq(
        new BidItem("0000001", "11-05-08-2016", "CA", 1.1),
        new BidItem("0000001", "11-05-08-2016", "MX", 1.5),
        new BidItem("0000001", "11-05-08-2017", "US", 1.55)
      )
    )

    val expected = sc.parallelize(
      Seq(
        new EnrichedItem("0000001", "Olinda Windsor Inn", "2016-08-05 11:00", "MX", 1.5),
        new EnrichedItem("0000001", "Olinda Windsor Inn", "2017-08-05 11:00", "US", 1.55))
    )

    val rdd = Runner.getEnriched(bids, motels)
    assertRDDEquals(rdd, expected)
  }

  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }


  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    Runner.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
