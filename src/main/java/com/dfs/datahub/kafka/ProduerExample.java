/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author sanjankar
 *
 */
public class ProduerExample {
	private final String topic;
	private final Properties props;

	public ProduerExample(final String brokers) {
		topic = "txn";
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
	}

	public void produce() {
		final String load = "pickle tiny rick hard cafe";
		final String[] split = load.split(" ");
		try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
			for (int i = 0; i < split.length; i++) {
				System.err.println(String.format("Pushing to topic %s, key = %s, value = %s", topic,
						Integer.toString(i), split[i] + "rigga ding ding"));
				producer.send(new ProducerRecord<>(topic, Integer.toString(i), split[i] + " bigga ding ding" + new Date().getTime()));
			}
		}
	}

	public static void main(final String[] args) {
		final String brokers = "localhost:9092";
		final ProduerExample c = new ProduerExample(brokers);
		c.produce();
	}
}
