import org.apache.spark.sql.functions.{col, dense_rank, desc, explode, lit, lower, rank, regexp_replace, row_number, substring_index, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
object Main {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.spark-project").setLevel(Level.WARN)
		println("Apache Spark Application Start ...")
		val spark = SparkSession.builder()
			.appName("Lab 2")
			.master(master = "local[*]")
			.config("spark.sql.autoBroadcastJoinThreshold", "-1")
			.getOrCreate()
		val xml_path = "C:\Users\admin\Documents\data\posts_sample.xml"
		val languages_path = "C:\Users\admin\Documents\data\programming-languages.csv"
		// Read the data
		val df_posts = spark.read
			.format("com.databricks.spark.xml")
			.option("rowTag", "row")
			.load(xml_path)
		val schema_languages = new StructType()
			.add("name", StringType, true)
			.add("wikipedia_url", StringType, true)
		val df_languages = spark.read.option("header", true).schema(schema_languages).csv(languages_path)
		///Select columns
		val df_data_body = df_posts.select("_Tags", "_CreationDate")
		df_data_body.printSchema()
		println()
		df_data_body.show(2)
		/// Create year column
		val df_years = df_data_body.select(col("_Tags"), substring_index(col("_CreationDate"), "-", 1).as("Year"))
		df_years.show(2)
		/// Replace tags
		val df_clean_temp = df_years.withColumn("_Tags", regexp_replace(df_years("_Tags") , "<", "" ))
		val df_clean = df_clean_temp.withColumn("_Tags", regexp_replace(df_clean_temp("_Tags") , ">", " " ))
		val df_drop_na = df_clean.na.drop()
		df_drop_na.show()
		///split
		val tuples = df_drop_na.withColumn("_Tags", explode(functions.split(df_drop_na("_Tags"), "[ ]")))
		tuples.show()
		/// to lowercase
		val df_languages_lowercase = df_languages.withColumn("name", lower(col("name")))
		df_languages_lowercase.show()
		/// join dataframes
		val df1 = tuples.toDF("_Tags", "Year")
		val df2 = df_languages_lowercase.toDF("name", "wikipedia_url")
		val result_df = df2.join(df1, df1("_Tags") === df2("name"), "inner").select(df2("name"), df1("Year"))
			.groupBy("Year", "name")
			.count
			.orderBy(desc("count"))
		result_df.show(10)
		// top 10
		val result_frame = result_df.withColumn("rank", rank().over(Window.partitionBy(result_df("Year")).orderBy(result_df("count").desc)))
			.filter(col("rank") <= 10).drop("rank")
		result_frame.show(30)
		// parquet
		result_frame.write.parquet("top_10_1.parquet")
		val parquet = spark.read.parquet("top_10_1.parquet")
		parquet.show()
	}
}
