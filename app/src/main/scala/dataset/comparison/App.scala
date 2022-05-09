package dataset.comparison

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import scala.util.matching.Regex


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dataset-comparison")
      .getOrCreate()

    val ds1 = spark.read.option("header", "true").option("inferschema", "true")
      .csv("src/main/resources/country_population.csv")
    val ds2 = spark.read.option("header", "true").option("inferschema", "true")
      .csv("src/main/resources/historic_indicators.csv")

    val pattern: Regex = """ \[.+\]""".r
    val columnNames = ds2.columns
    val newColumnNames = columnNames.map(pattern.replaceAllIn(_, ""))
    val renamedDs2 = ds2.columns.foldLeft(ds2) {
      (tmpDs, colName) => tmpDs.withColumnRenamed(colName, pattern.replaceAllIn(colName, ""))
    }

    val commonColumnNames = ds1.columns.intersect(renamedDs2.columns)

    println(s"Number of common columns: ${commonColumnNames.length}")
    println("Common columns:")
    commonColumnNames.foreach(println)
  }
}