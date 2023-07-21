package org.jacob;

import java.text.ParseException;

import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 多边形没有关闭的异常
 * 根据错误信息，出现了两个异常：Points of LinearRing do not form a closed linestring 和
 * java.lang.NullPointerException。
 * 
 * 首先，针对第一个异常 Points of LinearRing do not form a closed
 * linestring，这意味着线环的点没有形成一个封闭环。要解决这个异常，你需要检查线环的点坐标，确保第一个点和最后一个点是相同的。如果这两个点不同，那么你需要手动将最后一个点设置为与第一个点相同的坐标。
 * 
 * @author Zengmin.Zhang
 *
 */

/**
 * LINESTRING和POLYGON都是空间数据类型，用于表示几何对象。
 * 
 * 区别如下：
 * 
 * LINESTRING（线串）：LINESTRING是由一系列有序的点连接而成的线段。它由至少两个点组成，每个点都包含x和y坐标。LINESTRING表示线段、曲线或路径等抽象概念。
 * 
 * POLYGON（多边形）：POLYGON是由一系列有序的闭合线段连接而成的图形。它由至少三个点组成，首尾点相同，形成一个封闭的区域。POLYGON表示具有内部区域的平面图形，比如一个正方形或一个圆。
 * 
 * 可以将LINESTRING看作是一条线，而POLYGON则是一个封闭的多边形区域。
 * 
 * 以下是示例的LINESTRING和POLYGON表达：
 * 
 * LINESTRING(0 0, 1 1, 2 2) POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))
 * 
 * https://sedona.apache.org/1.3.1-incubating/api/flink/Function/#st_isclosed
 * 
 * @author Zengmin.Zhang
 *
 */
public class SparkSqlSedona {
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
				// 最后一条数据多边形没有关闭
				.load("data/geo.csv");// 文件位置

		rawDf.createOrReplaceTempView("rawdf");
		rawDf.show();
		System.out.println("======================================");

		spark.sql("DESCRIBE rawdf").show();

		spark.sql("DESCRIBE rawdf polygon").show();

		Dataset<Row> result = null;

		result = spark.sql(
				"SELECT ST_IsClosed(ST_GeomFromText(regexp_replace(polygon, '\"', ''))) AS isClosed FROM rawdf where 1=1 ");

		result.createOrReplaceTempView("test0");

		result.show();

		// 将字符串类型的字段转换为 Sedona 的 Geometry 类型
		result = spark.sql(
				"SELECT ST_GeomFromWKT(regexp_replace(polygon, '\"', '')) AS geo FROM rawdf where 1=1 and ST_IsClosed(ST_GeomFromText(regexp_replace(polygon, '\"', '')))");

		result.createOrReplaceTempView("test");

		// 显示查询结果
		result.show();

		System.out.println("======================================");
		// 经纬度
		result = spark.sql("SELECT ST_Point(123.2834,123.324) AS Point FROM test ");
		result.show();

		System.out.println("======================================");
		// 经纬度是否在一个多边形内
		result = spark
				.sql("SELECT ST_Contains(geo,ST_Point(13515248.838162526,3654281.857425429)) AS ST_Contains FROM test");
		result.show();

		// 经纬度是否在一个多边形内条件查询
		result = spark.sql(
				"SELECT  *  FROM test where ST_Contains(geo,ST_Point(13515248.838162526,3654281.857425429)) = true");
		result.show();

		// 查询是否关闭,使用该访问进行判断多边形是否关闭
		result = spark.sql("SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)'))");
		result.show();

		result = spark.sql("SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 0 0)'))");
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}