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
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.SaveMode
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.DataSourceReadOptions._
    import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter

    val tableName = "test_bd_video_logs"
    val basepath = "s3://aws-hadoop-test/test/hive/default/"

    val df = sql("select * from bd_video_logs")

    val data = df.withColumn(
      "cnt",
      row_number().over(
        Window.partitionBy("c_date").orderBy($"starttime".asc)
      )
    ).withColumn(
      "ref", concat_ws("_", $"c_date", $"cnt"
      )
    ).withColumn(
      "ts", lit(System.currentTimeMillis())
    )
    data.cache()
    data.count()
    data.show

    data.filter("c_date <= '2018-05-01' and c_date > '2018-04-25'").write.format("hudi").options(Map(
      RECORDKEY_FIELD_OPT_KEY -> "ref",                 //主键
      PARTITIONPATH_FIELD_OPT_KEY -> "c_date",          //分区字段
      PRECOMBINE_FIELD_OPT_KEY -> "ts",                 //预聚合字段(类似版本号)
      TABLE_NAME -> (tableName + "_hudi"),
      TABLE_TYPE_OPT_KEY -> COW_TABLE_TYPE_OPT_VAL     //读效率高、写效率低，用于经常读
//      TABLE_TYPE_OPT_KEY -> MOR_TABLE_TYPE_OPT_VAL     //写效率高、读效率低，用于经常写
    )).mode(SaveMode.Append).save(s"${basepath}${tableName}_hudi")


    // 读取快照
    val partitionDF = spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL     //读取快照
    )).load(s"${basepath}${tableName}_hudi/2018-04*")
    val allDF = spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL
    )).load(s"${basepath}${tableName}_hudi/*")

    // 增量读取(仅适用于COW_TABLE_TYPE_OPT_VAL模式写入的表)
    val formatter = DateTimeFormatter.ofPattern("YYYYMMddHHmmss")
    spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_INCREMENTAL_OPT_VAL,       //读取增量
      BEGIN_INSTANTTIME_OPT_KEY -> "0",                           //写入开始时间
      END_INSTANTTIME_OPT_KEY -> LocalDateTime.now().format(formatter)  //写入结束时间
    )).load(s"${basepath}${tableName}_hudi").show


    // 删除数据
    val delDF = allDF.selectExpr(
      "ref",
      "c_date",
      s"${System.currentTimeMillis()} ts"
    ).limit(2).toDF()

    delDF.cache()
    delDF.count()
    delDF.show()
//    |            ref|    c_date|     cnt|
//    +---------------+----------+--------+
//    |2018-04-30_4010|2018-04-30|11111110|
//    | 2018-04-30_126|2018-04-30|11111110|

    delDF.write.format("hudi").
      option(OPERATION_OPT_KEY,"delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "ref").
      option(PARTITIONPATH_FIELD_OPT_KEY, "c_date").
      option(TABLE_NAME, tableName).
      mode(SaveMode.Append).
      save(basepath+tableName+"_hudi")

    val afterDF = spark.read.format("hudi").options(Map(
      QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL
    )).load(s"${basepath}${tableName}_hudi/*")

    afterDF.filter("ref='2018-04-30_4010'").show

  }
}
