/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author sanjankar
 *
 */
public class Util {

	private Util() {
		/* Sonar. */
	}

	public static final Properties getProperties() {
		final Properties props = new Properties();
		final String serializer = StringSerializer.class.getName();
		final String deserializer = StringDeserializer.class.getName();
		final String brokers = "localhost:9092";
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
