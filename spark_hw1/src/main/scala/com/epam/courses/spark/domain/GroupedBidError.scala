package com.epam.courses.spark.domain

case class GroupedBidError(date: String, errorMessage: String, count: String) {

  override def toString: String = s"$date,$errorMessage,$count"
}
