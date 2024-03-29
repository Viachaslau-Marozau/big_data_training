package com.epam.bigdata.util

import org.apache.spark.rdd.RDD


object RddComparator
{
  def printDiff(expected: RDD[String], actual: RDD[String]) =
  {
    val actualArray = actual.collect
    val expectedArray = expected.collect
    val expectedDiff = expectedArray.filter(x => !actualArray.contains(x)).mkString("\n")
    val actualDiff = actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n")
    if (!expectedDiff.isEmpty || !actualDiff.isEmpty) {
      println("")
      println("EXPECTED elements NOT available in actual set")
      println(expectedArray.filter(x => !actualArray.contains(x)).mkString("\n"))
      println("---")
      println("ACTUAL elements NOT available in expected set")
      println(actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n"))
    } else {
      println("There were no differences between expected and actual RDDs")
    }
  }
}
