package org.jacob;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

public class SparkSqlGEO {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("GeoSparkExample").setMaster("local[*]");

		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 注册 GeoSparkSQL 函数
		GeoSparkSQLRegistrator.registerAll(spark);

		// 读取地理空间数据
		Dataset<Row> spatialData = spark.read().format("geospark").load("data/spatial-data.parquet");

		spatialData.createOrReplaceTempView("spatialData");
		
		// 执行地理空间查询
		Dataset<Row> result = spark.sql(
				"SELECT * FROM spatialData WHERE ST_Contains(ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), geometry)");

		// 显示查询结果
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}