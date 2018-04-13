/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

/**
 * @author sanjankar
 *
 */
public class ConsumerExample {
	private final String topic;
	private final Properties props;
	final Logger logger = LoggerFactory.getLogger(getClass());

	public ConsumerExample() {
		topic = "txn-op";
		logger.info("topic: {}", topic);
		props = Util.getProperties("consumer-example");
	}

	public void consume() {
		try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				final ConsumerRecords<String, String> records = consumer.poll(1000);
				for (final ConsumerRecord<String, String> record : records) {
					logger.info("Topic={} Partition={} offset={}, key={}, value={}", record.topic(), record.partition(),
							record.offset(), record.key(), Longs.fromByteArray(record.value().getBytes()));
				}
			}
		}
	}

	public static void main(final String[] args) {
		final ConsumerExample c = new ConsumerExample();
		c.consume();
	}
}
