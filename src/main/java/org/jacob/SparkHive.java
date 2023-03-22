package org.jacob;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHive {
	public static void main(String[] args) throws Exception {
		String outputFile = args[0];
		SparkConf conf = new SparkConf().setAppName("Java Spark SQL basic example");
		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		System.out.println("--------------------------------------------------------------------------");

		// spark.sql("use ddp_pro_dwh");
		Dataset<Row> sql = spark.sql("select * from ddp_pro_dwh.dely_delivery limit 10");
		//sql.show();
		if (outputFile == null) {
			outputFile = "/zzm/hiveOutput";
		}
		//sql.write().text(outputFile);
		sql.toJSON().write().text(outputFile);
		

		System.out.println("--------------------------------------------------------------------------" + sql.count());
	}
}
