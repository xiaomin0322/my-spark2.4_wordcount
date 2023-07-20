package org.jacob;

import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlReadByParquet {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("GeoSparkExample").setMaster("local[*]");

		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 注册 SedonaSQL 函数
		SedonaSQLRegistrator.registerAll(spark);

		
		Dataset<Row> parquet = spark.read().parquet("path/to/output_file.parquet");
		
		parquet.show();

		// 停止 SparkContext
		spark.stop();
	}
}