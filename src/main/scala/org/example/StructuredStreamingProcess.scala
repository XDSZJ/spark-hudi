package org.example

import com.alibaba.fastjson.JSON
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.beans.BeanProperty

object StructuredStreamingProcess {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("javalin").setLevel(Level.ERROR)
  Logger.getLogger("hive").setLevel(Level.WARN)
  private final val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * 获取SparkSession
   *
   * @return [[SparkSession]]
   */
  def getSparkSession: SparkSession = {
    SparkSession.builder().appName("structured_streaming_hudi_hive")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.receiver.maxRate", "1024")
      .config("spark.streaming.kafka.maxRatePerPartition", "256")
      .config("spark.streaming.kafka.consumer.poll.ms", "4096")
      .config("spark.streaming.backpressure.enabled", value = true)
      .config("spark.dynamicAllocation.enabled", value = true)
      .config("spark.default.parallelism", "2")
      .config("spark.sql.shuffle.partitions", "2")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * 定义kafka数据流
   *
   * @param sparkSession [[SparkSession]]
   * @return [[DataFrame]]
   */
  def getDataStream(sparkSession: SparkSession): DataFrame = {
    sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "172.16.112.71:9092,172.16.112.72:9092,172.16.112.73:9092,172.16.112.74:9092,172.16.112.75:9092,172.16.112.76:9092,172.16.112.77:9092")
      .option("subscribe", "ax_user_test")
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 10000)
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr(
        "topic as kafka_topic",
        "CAST(partition AS STRING) kafka_partition",
        "cast(timestamp as String) kafka_timestamp",
        "CAST(offset AS STRING) kafka_offset",
        "CAST(key AS STRING) kafka_key",
        "CAST(value AS STRING) kafka_value",
        "current_timestamp() current_time"
      )
      .selectExpr("kafka_value",
        "kafka_timestamp",
        "substr(current_time,1,10) partition_date")
  }

  def query(dataFrame: DataFrame, sparkSession: SparkSession): StreamingQuery = {
    import sparkSession.implicits._
    dataFrame.mapPartitions(partitions => {
      partitions.map(partition => {
        val kafka_value: String = partition.getString(0)
        JSON.parseObject(kafka_value, classOf[AX_User])
      })
    }).toDF()
      .withColumn("kafka_timestamp", unix_timestamp(col("update_time"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("partition_date", from_unixtime(unix_timestamp(), "yyyy-MM-dd"))
      .writeStream
      .queryName("ax_user")
      .foreachBatch((batchDF: DataFrame, _: Long) => {
        //        if (!batchDF.isEmpty) {
        batchDF.write.format("org.apache.hudi")
          .option("hoodie.insert.shuffle.parallelism", "2")
          .option("hoodie.upsert.shuffle.parallelism", "2")
          .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, HoodieTableType.MERGE_ON_READ.name) // 配置读时合并
          .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "ax_uid") // 唯一id列名，可以指定多个字段
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp") //指定更新字段，该字段数值大的会覆盖小的
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_date") // 指定 partitionpath
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") // 设置数据集注册并同步到hive
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "ods") // hive database
          .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "ax_user") // hive table
          .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "partition_date") // hive表分区字段
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://172.16.112.76:10000") // hiveserver2 地址
          .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor") // 从partitionpath中提取hive分区对应的值，MultiPartKeysValueExtractor使用的是"/"分割
          .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") // 当前数据的分区变更时，数据的分区目录是否变化
          .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
          .option(HoodieWriteConfig.TABLE_NAME, "ax_user") // hudi table名
          .mode(SaveMode.Append)
          .save("/user/hive/warehouse/ods.db/ax_user")
        //        }
      }).option("checkpointLocation", "/checkpoint/structStreaming/structured_streaming_hudi_hive/")
      .start()
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = getSparkSession
    val dataFrame: DataFrame = getDataStream(session)

    val sc: SparkContext = session.sparkContext
    val text: String =
      s"""
         |### Structured Streaming HUDI Hive \n
         |> sparkUser: ${sc.sparkUser}  \n
         |> startTime: ${sc.startTime}  \n
         |> appName: ${sc.appName}  \n
         |> deployMode: ${sc.deployMode}  \n
         |> master: ${sc.master}  \n
         |>> uiWebUrl: ${sc.uiWebUrl.get}  \n
         |> applicationId: ${sc.applicationId}  \n
         |>> @18621771210
         |""".stripMargin
    //    Monitoring.sendMarkdown("Structured Streaming Monitoring", text,
    //      Collections.singletonList("18621771210"), isAtAll = false)

    val streamingQuery: StreamingQuery = query(dataFrame, session)
    streamingQuery.awaitTermination()
  }
}

case class AX_User(
                    @BeanProperty ax_uid: Int,
                    @BeanProperty mobile: String,
                    @BeanProperty password: String,
                    @BeanProperty password_old: String,
                    @BeanProperty user_type_bits: Int,
                    @BeanProperty status: String,
                    @BeanProperty create_time: String,
                    @BeanProperty update_time: String
                  )