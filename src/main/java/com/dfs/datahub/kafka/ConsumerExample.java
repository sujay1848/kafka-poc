/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.primitives.Longs;

/**
 * @author sanjankar
 *
 */
public class ConsumerExample {
	private final String topic;
	private final Properties props;

	public ConsumerExample(final String brokers) {
		topic = "txn-op";
		System.out.println("topic: " + topic);
		final String serializer = StringSerializer.class.getName();
		final String deserializer = StringDeserializer.class.getName();
		props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", deserializer);
		props.put("value.deserializer", deserializer);
		props.put("key.serializer", serializer);
		props.put("value.serializer", serializer);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
	}

	public void consume() {
		try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				final ConsumerRecords<String, String> records = consumer.poll(1000);
				for (final ConsumerRecord<String, String> record : records) {
					System.out.println(new Date().toString()
							+ String.format(" Topic=%s Partition=%s offset=%s, key=%s, value=\"%s\"", record.topic(),
									record.partition(), record.offset(), record.key(), Longs.fromByteArray(record.value().getBytes())));
				}
			}
		}
	}

	public static void main(final String[] args) {
		final String brokers = "localhost:9092";
		final ConsumerExample c = new ConsumerExample(brokers);
		c.consume();
	}
}
