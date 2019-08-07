package com.epam.courses.spark.sql

import java.io.File

import com.epam.courses.spark.sql.Runner._
import com.epam.courses.spark.sql.Constants._
import RunnerTest._
import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder
import com.epam.bigdata.util.RddComparator
import org.apache.spark.sql.Row


class RunnerTest {
  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_INTEGRATION = "/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "/input/motels.gz.parquet"

  val INTEGRATION_PATH = "src/test/resources/integration"
  val INTEGRATION_ERROR_PATH = "src/test/resources/integration_error"
  val INTEGRATION_WITHOU_ERROR_PATH = "src/test/resources/integration_no_error"

  val INPUT_BIDS_FIRST_SAMPLE = "src/test/resources/bids_sample_1.gz.parquet"
  val INPUT_BIDS_SECOND_SAMPLE = "src/test/resources/bids_sample_2.gz.parquet"

  val INPUT_EXCHANGE_RATES_SAMPLE = "src/test/resources/exchange_rate_1.txt"
  val INPUT_EXCHANGE_RATES_SAMPLE2 = "src/test/resources/exchange_rate_2.txt"

  val INPUT_MOTELS_FIRST_SAMPLE = "src/test/resources/motels_sample_1.gz.parquet"



  private var outputFolder: File = null

  @Before
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def testDetectMaxPrice() = {
    val bidRDD = sc.parallelize(
      Seq(
        Row("0000001", "15-04-08-2016", "CA", "1.90"),
        Row("0000001", "15-04-08-2016", "US", "1.92"),
        Row("0000001", "15-04-08-2016", "MX", "0.92")
      )
    )

    val expected = sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn", "2016-08-04 15:00", "US", 1.92)
      )
    )

    val bidDF = sqlContext.createDataFrame(bidRDD, BIDS_HEADER)
    val motels = Runner.getMotels(sqlContext, INPUT_MOTELS_FIRST_SAMPLE)
    val items = Runner.getEnriched(bidDF, motels, false)

    RDDComparisons.assertRDDEquals(items.rdd, expected)
  }

  @Test
  def testDetectMaxPriceWithDublicates() = {
    val bidRDD = sc.parallelize(
      Seq(
        Row("0000001", "15-04-08-2016", "CA", "1.92"),
        Row("0000001", "15-04-08-2016", "US", "1.92"),
        Row("0000001", "15-04-08-2016", "MX", "0.92")
      )
    )

    val expected = sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn", "2016-08-04 15:00", "CA", 1.92),
        Row("0000001", "Olinda Windsor Inn", "2016-08-04 15:00", "US", 1.92)
      )
    )

    val bidDF = sqlContext.createDataFrame(bidRDD, BIDS_HEADER)
    val motels = Runner.getMotels(sqlContext, INPUT_MOTELS_FIRST_SAMPLE)
    val items = Runner.getEnriched(bidDF, motels, false)

    RDDComparisons.assertRDDEquals(items.rdd, expected)
  }

  @Test
  def testDetectMaxPriceWithRemoveDublicates() = {
    val bidRDD = sc.parallelize(
      Seq(
        Row("0000001", "15-04-08-2016", "CA", "1.92"),
        Row("0000001", "15-04-08-2016", "US", "1.92"),
        Row("0000001", "15-04-08-2016", "MX", "0.92")
      )
    )

    val expected = sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn", "2016-08-04 15:00", "US", 1.92)
      )
    )

    val bidDF = sqlContext.createDataFrame(bidRDD, BIDS_HEADER)
    val motels = Runner.getMotels(sqlContext, INPUT_MOTELS_FIRST_SAMPLE)
    val items = Runner.getEnriched(bidDF, motels, true)

    RDDComparisons.assertRDDEquals(items.rdd, expected)
  }

  @Test
  def shouldReadRawBids() = {
    val expected = sc.parallelize(
      Seq(
        Row("0000002", "15-04-08-2016", "0.89", "0.92"),
        Row("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL", null)
      )
    )

    val rawBids = Runner.getRawBids(sqlContext, INPUT_BIDS_FIRST_SAMPLE)
    RDDComparisons.assertRDDEquals(rawBids.rdd, expected)
  }

  @Test
  def shouldCollectErroneousRecords() = {
    val expected = sc.parallelize(
      Seq(
        Row("06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL", 1)
      )
    )

    val rawBids = Runner.getRawBids(sqlContext, INPUT_BIDS_FIRST_SAMPLE)
    val erroneous = Runner.getErroneousRecords(rawBids)
    RDDComparisons.assertRDDEquals(expected, erroneous.rdd)
  }

  @Test
  def shouldCollectBids() = {
    val expected = sc.parallelize(
      Seq(
        Row("0000003", "05-04-05-2016", "US", 0.5),
        Row("0000003", "13-24-12-2015", "US", 2.0),
        Row("0000003", "05-04-05-2016", "CA", 0.5),
        Row("0000003", "06-25-03-2016", "CA", 1.5),
        Row("0000003", "06-25-03-2016", "MX", 2.2)
      )
    )

    val rawBids = Runner.getRawBids(sqlContext, INPUT_BIDS_SECOND_SAMPLE)
    val exchangeRates = Runner.getExchangeRates(sqlContext, INPUT_EXCHANGE_RATES_SAMPLE2)
    val bids = Runner.getBids(rawBids, exchangeRates)
    RDDComparisons.assertRDDEquals(bids.rdd, expected)
  }

  @Test
  def shouldReadExchangeRates() = {
    val expected = sc.parallelize(
      Seq(
        Row("11-06-05-2016", "Euro", "EUR", "0.803"),
        Row("11-05-08-2016", "Euro", "EUR", "0.873"),
        Row("11-05-08-2017", "Euro", "EUR", "0.500")
      )
    )

    val exchangeRates = Runner.getExchangeRates(sqlContext, INPUT_EXCHANGE_RATES_SAMPLE)
    RDDComparisons.assertRDDEquals(exchangeRates.rdd, expected)
  }

  @Test
  def shouldReadMotels() = {
    val expected = sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn"),
        Row("0000002", "Merlin Por Motel"),
        Row("0000003", "Olinda Big River Casino")
      )
    )

    val motels = Runner.getMotels(sqlContext, INPUT_MOTELS_FIRST_SAMPLE)
    RDDComparisons.assertRDDEquals(motels.rdd, expected)
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregatesErrorsWithoutErrors() = {

    runIntegrationTest(INTEGRATION_WITHOU_ERROR_PATH)
    val EXPECTED_AGGREGATED_INTEGRATION = INTEGRATION_WITHOU_ERROR_PATH + "/expected_output/aggregated"
    val EXPECTED_ERRORS_INTEGRATION = INTEGRATION_WITHOU_ERROR_PATH + "/expected_output/expected_sql"

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregatesErrorsOnlyErrors() = {

    runIntegrationTest(INTEGRATION_ERROR_PATH)
    val EXPECTED_AGGREGATED_INTEGRATION = INTEGRATION_ERROR_PATH + "/expected_output/aggregated"
    val EXPECTED_ERRORS_INTEGRATION = INTEGRATION_ERROR_PATH + "/expected_output/expected_sql"

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregatesErrors() = {

    runIntegrationTest(INTEGRATION_PATH)
    val EXPECTED_AGGREGATED_INTEGRATION = INTEGRATION_PATH + "/expected_output/aggregated"
    val EXPECTED_ERRORS_INTEGRATION = INTEGRATION_PATH + "/expected_output/expected_sql"

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest(path: String) = {
    Runner.
      processData(sqlContext, path + INPUT_BIDS_INTEGRATION, path + INPUT_MOTELS_INTEGRATION, path + INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RDDComparisons.assertRDDEquals(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object RunnerTest {
  var sc: SparkContext = null
  var sqlContext: HiveContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("spark_hw2 test"))
    sqlContext = new HiveContext(sc)
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }
}
