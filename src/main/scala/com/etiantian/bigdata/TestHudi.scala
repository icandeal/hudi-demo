package com.etiantian.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Hello world!
 *
 */
object TestHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestHudi")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val tableName = "test_bd_video_logs"
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.SaveMode
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.DataSourceReadOptions._
    import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter

    val df = sql("select * from bd_video_logs")

    val data = df.withColumn(
      "cnt",
      row_number().over(
        Window.partitionBy("c_date").orderBy($"starttime".asc)
      )
    ).withColumn("ref", concat_ws("_", $"c_date", $"cnt"))
    data.cache()
    data.count()
    data.show

    data.filter("c_date <= '2018-05-01' and c_date > '2018-04-25'").write.format("hudi").options(Map(
      RECORDKEY_FIELD_OPT_KEY -> "ref",                 //主键
      PARTITIONPATH_FIELD_OPT_KEY -> "c_date",          //分区字段
      PRECOMBINE_FIELD_OPT_KEY -> "cnt",                //预聚合字段
      TABLE_NAME -> (tableName + "_hudi"),
      TABLE_TYPE_OPT_KEY -> COW_TABLE_TYPE_OPT_VAL     //读效率高、写效率低，用于经常读
//      TABLE_TYPE_OPT_KEY -> MOR_TABLE_TYPE_OPT_VAL     //写效率高、读效率低，用于经常写
    )).mode(SaveMode.Append).save(s"s3://aws-hadoop-test/test/hive/default/${tableName}_hudi")


    // 读取快照
    val partitionDF = spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL     //读取快照
    )).load(s"s3://aws-hadoop-test/test/hive/default/${tableName}_hudi/2018-04*")
    val allDF = spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL
    )).load(s"s3://aws-hadoop-test/test/hive/default/${tableName}_hudi/*")

    // 增量读取
    val formatter = DateTimeFormatter.ofPattern("YYYYMMddHHmmss")
    spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_INCREMENTAL_OPT_VAL,       //读取增量
      BEGIN_INSTANTTIME_OPT_KEY -> "0",                           //写入开始时间
      END_INSTANTTIME_OPT_KEY -> LocalDateTime.now().format(formatter)  //写入结束时间
    )).load(s"s3://aws-hadoop-test/test/hive/default/${tableName}_hudi").show
  }
}
