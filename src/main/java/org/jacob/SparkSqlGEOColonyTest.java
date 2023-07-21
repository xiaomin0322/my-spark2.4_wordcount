package org.jacob;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

public class SparkSqlGEOColonyTest {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("SparkSqlGEOColonyTest");
		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		// 注册 GeoSparkSQL 函数
		GeoSparkSQLRegistrator.registerAll(spark);
		
		String sql  = "SELECT st_contains(t.pg,st_point(cast(o.longitude as decimal(15,2)), cast(o.latitude  as decimal(15,2)))),  *\r\n" + 
				" FROM (  \r\n" + 
				"   SELECT *  \r\n" + 
				"   FROM ddp_pro_ods.kiwi_track_order o  \r\n" + 
				"   WHERE 1=1   \r\n" + 
				" ) AS o  \r\n" + 
				" LEFT JOIN (   \r\n" + 
				"   SELECT m.pos_code,  objid \r\n" + 
				"   FROM ddp_pro_ods.KIWI_MYSITE m  \r\n" + 
				" ) AS m ON m.pos_code = o.StoreId\r\n" + 
				" LEFT JOIN (\r\n" + 
				"   SELECT t.mysite_objid, ST_GeomFromWKT(t.pg) as pg\r\n" + 
				"   FROM ddp_pro_ods.KIWI_TRADEZONEELEMENT t \r\n" + 
				"   where t.pg is not null \r\n" + 
				"   and  t.pg != \"\"\r\n" + 
				"   ) AS t ON t.mysite_objid = m.objid   \r\n" + 
				"   WHERE 1=1  \r\n" + 
				"  and  pg is not null\r\n" + 
				"  and st_contains(t.pg,st_point(cast(o.longitude as decimal(15,2)), cast(o.latitude  as decimal(15,2)))) = true limit 200";
		
		// 执行地理空间查询
		Dataset<Row> result = spark.sql(
				sql);
		// 显示查询结果
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}