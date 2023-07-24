package org.jacob;

import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlSedonaColonyTest {
	public static void main(String[] args) {
		// 创建 SparkConf 对象
		SparkConf conf = new SparkConf().setAppName("SparkSqlSedonaColonyTest");
		// 创建 SparkSession
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		// 注册 SedonaSQLRegistrator 函数
		SedonaSQLRegistrator.registerAll(spark);
		
		// 获取限制值
		int limitValue = Integer.parseInt(conf.get("spark.limit", "200")); // 默认值为 200
		
		String sql  = "SELECT *" + 
				" FROM ("+ 
				" SELECT * " + 
				" FROM ddp_pro_ods.kiwi_track_order o " + 
				" WHERE 1=1 " + 
				" ) AS o" + 
				" LEFT JOIN (   " + 
				"   SELECT m.pos_code,  m.objid" + 
				"   FROM ddp_pro_ods.KIWI_MYSITE m  " + 
				" ) AS m ON m.pos_code = o.StoreId" + 
				" LEFT JOIN (" + 
				"   SELECT t.objid AS radezoneelement_objid,t.mysite_objid, ST_GeomFromWKT(t.pg) as pg" + 
				"   FROM ddp_pro_ods.KIWI_TRADEZONEELEMENT_2 t " + 
				"   where t.pg is not null " + 
				"   and  t.pg1 is not null" + 
				"   and  ST_IsClosed(ST_GeomFromText(t.pg1))" + 
				"   ) AS t ON t.mysite_objid = m.objid" + 
				" WHERE 1=1  " + 
				"  and t.pg is not null and o.longitude is not null and o.latitude is not null" + 
				"  and st_contains(t.pg,st_point(cast(o.longitude as decimal(15,2)), cast(o.latitude  as decimal(15,2)))) = true limit "+limitValue;
		
		// 执行地理空间查询
		Dataset<Row> result = spark.sql(
				sql);
		// 显示查询结果
		result.show();

		// 停止 SparkContext
		spark.stop();
	}
}