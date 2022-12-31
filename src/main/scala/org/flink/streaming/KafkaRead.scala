package org.flink.streaming

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object KafkaRead {
  def main(args: Array[String]) {
    val TOPIC = "kafkaproducer"
    val config = ConfigFactory.load("kafka.consumer.conf").getConfig("confighome")
    val kafkaconfig = config.getConfig("kafka-consumer")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    val stream:DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
    stream.print()
    env.execute("Azure EventHub Kafka Read Example!")
  }
}
