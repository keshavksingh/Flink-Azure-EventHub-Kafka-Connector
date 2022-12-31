package org.flink.streaming

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

import java.util.Properties
import scala.io.Source

object KafkaReadWrite {
  def main(args: Array[String]) {

    val TOPIC = "kafkaconsumer"
    val config = ConfigFactory.load("kafka.consumer.conf").getConfig("confighome")
    val kafkaconfig = config.getConfig("kafka-consumer")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
        val kafkaSource = KafkaSource.builder()
          .setBootstrapServers(kafkaconfig.getString("bootstrap.servers"))
          .setProperty("sasl.mechanism", kafkaconfig.getString("sasl.mechanism"))
          .setProperty("sasl.jaas.config", kafkaconfig.getString("sasl.jaas.config"))
          .setProperty("security.protocol", kafkaconfig.getString("security.protocol"))
          .setTopics(TOPIC)
          .setGroupId("$Default")
          .setStartingOffsets(OffsetsInitializer.earliest())
          .setValueOnlyDeserializer(new SimpleStringSchema())
          .build()
        val stream: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
        stream.print()

    /* Write To Kafka
    To Write - Convert the Config to Property
    */
    val WriteTopic = "kafkaproducer"

    val serializer = KafkaRecordSerializationSchema.builder()
      .setTopic(WriteTopic)
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()

    val  propFromConfig = new Properties()
    /* Properties can be Hard-Coded as below or Optionally be Re-trived from (kafka.producer.properties) Properties File.
    propFromConfig.setProperty("bootstrap.servers","deeventhub01.servicebus.windows.net:9093")
    propFromConfig.setProperty("sasl.mechanism","PLAIN")
    propFromConfig.setProperty("transaction.timeout.ms","700000")
    propFromConfig.setProperty("security.protocol","SASL_SSL")
    propFromConfig.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://deeventhub01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=6KGZwbpS4PDCd7iIpOlVrcaaCOo3L1sdLKYU+nmGRdE=\";")
    propFromConfig.setProperty("client.id","kafkaproducer")
    */
    val source = Source.fromFile("src/main/resources/kafka.producer.properties")
    propFromConfig.load(source.bufferedReader())

    //println(propFromConfig.getProperty("transaction.timeout.ms"))
    /*
    Either of KafkaSink or KafkaSinkBuilder Option Works to Send To Kafka Producer
     */
    val KafkaSinkBuilder = sink.KafkaSink.builder()
      .setBootstrapServers(kafkaconfig.getString("bootstrap.servers"))
      .setKafkaProducerConfig(propFromConfig)
      .setDeliverGuarantee(DeliveryGuarantee.NONE)
      .setRecordSerializer(serializer)
      .setTransactionalIdPrefix("mytransationIdPrefex")
      .build()

    stream.sinkTo(KafkaSinkBuilder)
    /*
        val KafkaSink = sink.KafkaSink.builder()
          .setBootstrapServers(kafkaconfig.getString("bootstrap.servers"))
          .setKafkaProducerConfig(propFromConfig)
          .setRecordSerializer(serializer)
          .setDeliverGuarantee(DeliveryGuarantee.NONE)
          .setTransactionalIdPrefix("mytransationIdPrefex")
          .build()

        dataStream.sinkTo(KafkaSink)
        */

    env.execute("Azure EventHub Kafka Read & Write Example!")

  }
}
