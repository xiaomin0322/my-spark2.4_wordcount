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

		Dataset<Row> rawDf = spark.read().format("csv")
			    .option("header", "true")  // 指定第一行作为列名
			    .option("inferSchema", "true")  // 推断列的数据类型
			    .option("delimiter", ",")  // 指定列分隔符，默认为逗号
				.load("data/geo.csv");//文件位置
		
		
		rawDf.createOrReplaceTempView("rawdf");
		rawDf.show();
		System.out.println("======================================");
		
		spark.sql("DESCRIBE rawdf").show();
		
		spark.sql("DESCRIBE rawdf polygon").show();

		// 将字符串类型的字段转换为 Sedona 的 Geometry 类型
		Dataset<Row> result = spark.sql("SELECT ST_GeomFromWKT(regexp_replace(polygon, '\"', '')) AS geo FROM rawdf ");
		
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
		
		//经纬度是否在一个多边形内条件查询
		result = spark.sql("SELECT  *  FROM test where ST_Contains(geo,ST_Point(13515248.838162526,3654281.857425429)) = true");
		result.show();
		

		// 停止 SparkContext
		spark.stop();
	}
}