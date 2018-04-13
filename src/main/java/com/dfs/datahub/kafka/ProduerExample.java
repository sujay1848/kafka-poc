/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sanjankar
 *
 */
public class ProduerExample {
	private final Properties props;
	final Logger logger = LoggerFactory.getLogger(getClass());
	private static final String TOPIC = "txn";

	public ProduerExample() {
		logger.info("topic: {}", TOPIC);
		props = Util.getProperties();
	}

	public void produce() {
		final String load = "pickle tiny rick hard cafe";
		final String[] split = load.split(" ");
		try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
			for (int i = 0; i < split.length; i++) {
				logger.info("Pushing to topic {}, key = {}, value = {}", TOPIC, Integer.toString(i),
						split[i] + " bigga ding ding");
				producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i),
						split[i] + " bigga ding ding" + new Date().getTime()));
			}
		}
	}

	public static void main(final String[] args) {
		final ProduerExample c = new ProduerExample();
		c.produce();
	}
}
