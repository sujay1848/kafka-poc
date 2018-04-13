/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

/**
 * @author sanjankar
 *
 */
public class Util {

	private Util() {
		/* Sonar. */
	}

	public static final Properties getProperties(final String applicationId) {
		final Properties props = new Properties();
		final String serializer = StringSerializer.class.getName();
		final String deserializer = StringDeserializer.class.getName();
		final String brokers = "localhost:9092";
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
		props.put("bootstrap.servers", brokers);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", deserializer);
		props.put("value.deserializer", deserializer);
		props.put("key.serializer", serializer);
		props.put("value.serializer", serializer);
		return props;
	}
}
