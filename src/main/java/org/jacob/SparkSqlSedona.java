package org.jacob;

import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlSedona {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("GeoSparkExample").setMaster("local[*]");

		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 注册 SedonaSQL 函数
		SedonaSQLRegistrator.registerAll(spark);

		Dataset<Row> rawDf = spark.read().format("csv").option("delimiter", "\t").option("header", "false")
				.load("data/checkin2.tsv");
		
		
		rawDf.createOrReplaceTempView("rawdf");
		rawDf.show();
		System.out.println("======================================");
		
		spark.sql("DESCRIBE rawdf").show();
		
		spark.sql("DESCRIBE rawdf _c0").show();

		// 将字符串类型的字段转换为 Sedona 的 Geometry 类型
		Dataset<Row> result = spark.sql("SELECT ST_GeomFromWKT(_c0) AS geo FROM rawdf ");
		
		result.createOrReplaceTempView("test");
		// 显示查询结果
		result.show();

		System.out.println("======================================");
		// 经纬度
		result = spark.sql("SELECT ST_Point(123.2834,123.324) AS Point FROM test ");
		result.show();
		
		System.out.println("======================================");
		// 经纬度是否在一个多边形内
		result = spark.sql("SELECT ST_Contains(geo,ST_Point(13515248.838162526,3654281.857425429)) AS ST_Contains FROM test");
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}