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
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		// 注册 GeoSparkSQL 函数
		GeoSparkSQLRegistrator.registerAll(spark);
		
		String sql  = "SELECT *" + 
				" FROM ("+ 
				" SELECT * " + 
				" FROM ddp_pro_ods.kiwi_track_order o " + 
				" WHERE 1=1 " + 
				" ) AS o" + 
				" LEFT JOIN (   " + 
				"   SELECT m.pos_code,  objid " + 
				"   FROM ddp_pro_ods.KIWI_MYSITE m  " + 
				" ) AS m ON m.pos_code = o.StoreId" + 
				" LEFT JOIN (" + 
				"   SELECT t.mysite_objid, ST_GeomFromWKT(t.pg) as pg" + 
				"   FROM ddp_pro_ods.KIWI_TRADEZONEELEMENT_2 t " + 
				"   where t.pg is not null " + 
				"   and  t.pg1 is not null" + 
				"   and  ST_IsClosed(ST_GeomFromText(t.pg1))" + 
				"   ) AS t ON t.mysite_objid = m.objid" + 
				" WHERE 1=1  " + 
				"  and t.pg is not null and o.longitude is not null and o.latitude is not null" + 
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