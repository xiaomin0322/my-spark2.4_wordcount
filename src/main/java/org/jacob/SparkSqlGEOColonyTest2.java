package org.jacob;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

public class SparkSqlGEOColonyTest2 {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("SparkSqlGEOColonyTest2");
		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		// 注册 GeoSparkSQL 函数
		GeoSparkSQLRegistrator.registerAll(spark);
		
		String sql  = "  SELECT *  \r\n" + 
				"   FROM ddp_pro_ods.kiwi_track_order o  \r\n" + 
				"   WHERE 1=1  limit 200";
		
		// 执行地理空间查询
		Dataset<Row> result = spark.sql(
				sql);
		// 显示查询结果
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}