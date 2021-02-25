package com.lab1
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{countDistinct, desc}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.{Logger, Level}
object lab1 {
	def main(args: Array[String]): Unit = {
		BasicConfigurator.configure ()
		println("Apache Spark Application Start ...")
		val conf = new SparkConf()
			.setMaster("yarn")
		val spark = SparkSession.builder()
			.appName("Lab 1")
			.config(conf)
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.spark-project").setLevel(Level.WARN)
		val schema_trips = new StructType()
			.add("tripId", IntegerType, true)
			.add("duration", IntegerType, true)
			.add("startDate", DataTypes.DateType, true)
			.add("startStation", StringType, true)
			.add("startTerminal", IntegerType, true)
			.add("endtDate", DataTypes.DateType, true)
			.add("endStation", StringType, true)
			.add("endTerminal", IntegerType, true)
			.add("bikeId", IntegerType, true)
			.add("subscriptionType", StringType, true)
			.add("zipCode", IntegerType, true)
		val csv_file_path_trips = "C:/Users/admin/Documents/data/trips.csv"
		val trips_df = spark.read.option("header", true).schema(schema_trips).csv(csv_file_path_trips)
		trips_df.show(numRows = 10, truncate = false)
		trips_df.printSchema()
		val schema_stations = new StructType()
			.add("stationId", IntegerType, true)
			.add("name", StringType, true)
			.add("lat", DoubleType, true)
			.add("long", DoubleType, true)
			.add("dockcount", IntegerType, true)
			.add("landmark", StringType, true)
			.add("installation", StringType, true)
		val csv_file_path_stations = "C:/Users/admin/Documents/data/stations.csv"
		val stations_df = spark.read.option("header", true).schema(schema_stations).csv(csv_file_path_stations)
		stations_df.show(numRows = 10, truncate = false)
		stations_df.printSchema()
		// Task 1
		trips_df.agg(functions.max(trips_df(trips_df.columns(1)))).show()
		trips_df.orderBy((desc("duration"))).limit(1).show()
		// Task 2
		val task_2 = stations_df.crossJoin(stations_df).rdd.map { row =>
			calcDist(
				row.get(2).toString.toDouble,
				row.get(3).toString.toDouble,
				row.get(9).toString.toDouble,
				row.get(10).toString.toDouble
			)
		}.max()
		println(task_2)
		println()
		// Task 3

		trips_df.groupBy("bikeId").agg(functions.sum("duration")).orderBy((desc("sum(duration)"))).limit(1).show()
		// Task 4
		trips_df.agg(countDistinct("bikeId")).show()
		// Task 5
		trips_df.groupBy("bikeId").agg(functions.sum("duration")).filter("sum(duration) > 180").show()
		println(trips_df.groupBy("tripId").agg(functions.sum("duration")).count())
		trips_df.groupBy("tripId").agg(functions.sum("duration")).filter("sum(duration) > 180").show()
	}
	def calcDist(lat1: Double, long1: Double, lat2: Double, long2: Double): Double = {
		val latDistance = Math.toRadians(lat1 - lat2)
		val lngDistance = Math.toRadians(long1 - long2)
		val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)
		val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
		return Math.round(6371000L * c).asInstanceOf[Int] // 6371 is an Earth radius in km
	}
}
