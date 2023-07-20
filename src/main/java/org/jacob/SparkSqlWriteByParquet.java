package org.jacob;

import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlWriteByParquet {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("GeoSparkExample").setMaster("local[*]");

		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 注册 SedonaSQL 函数
		SedonaSQLRegistrator.registerAll(spark);

		Dataset<Row> rawDf = spark.read().format("csv").option("header", "true") // 指定第一行作为列名
				.option("inferSchema", "true") // 推断列的数据类型
				.option("delimiter", ",") // 指定列分隔符，默认为逗号
				.load("data/spatial-data.parquet");// 文件位置

		rawDf.createOrReplaceTempView("rawdf");
		rawDf.show();
		System.out.println("======================================");
		
		 // 重命名列名
		rawDf = rawDf.withColumnRenamed("name   age   city", "name_age_city");

        
		//将数据保存为parquet
		rawDf.write().parquet("path/to/output_file.parquet");
		
		//Dataset<Row> parquet = spark.read().parquet("path/to/output_file.parquet");
		
		//parquet.show();

		// 停止 SparkContext
		spark.stop();
	}
}