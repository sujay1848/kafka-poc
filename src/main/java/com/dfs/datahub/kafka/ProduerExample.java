/**
 *
 */
package com.dfs.datahub.kafka;

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
		props = Util.getProperties("producer-example");
	}

	public void produce() {
		final String load = "There's a lady who's sure\r\n" +
				"All that glitters is gold\r\n" +
				"And she's buying a stairway to heaven\r\n" +
				"When she gets there she knows\r\n" +
				"If the stores are all closed\r\n" +
				"With a word she can get what she came for\r\n" +
				"Oh oh oh oh and she's buying a stairway to heaven\r\n" +
				"There's a sign on the wall\r\n" +
				"But she wants to be sure\r\n" +
				"'Cause you know sometimes words have two meanings\r\n" +
				"In a tree by the brook\r\n" +
				"There's a songbird who sings\r\n" +
				"Sometimes all of our thoughts are misgiving\r\n" +
				"Ooh, it makes me wonder\r\n" +
				"Ooh, it makes me wonder\r\n" +
				"There's a feeling I get\r\n" +
				"When I look to the west\r\n" +
				"And my spirit is crying for leaving\r\n" +
				"In my thoughts I have seen\r\n" +
				"Rings of smoke through the trees\r\n" +
				"And the voices of those who standing looking\r\n" +
				"Ooh, it makes me wonder\r\n" +
				"Ooh, it really makes me wonder";
		final String[] split = load.split(" ");
		try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
			for (int i = 0; i < split.length; i++) {
				logger.info("Pushing to topic {}, key = {}, value = {}", TOPIC, Integer.toString(i), split[i]);
				producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), split[i]));
			}
		}
	}

	public static void main(final String[] args) {
		final ProduerExample c = new ProduerExample();
		c.produce();
	}
}
