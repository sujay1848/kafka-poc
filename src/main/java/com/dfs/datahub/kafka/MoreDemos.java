/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author sanjankar
 *
 */
public class MoreDemos {

	public static void main(final String[] args) {
		try {
			final String brokers = "localhost:9092";
			final Properties props = new Properties();
			final Serde<String> stringSerde = Serdes.String();
			final Serde<Long> longSerde = Serdes.Long();
			props.put("bootstrap.servers", brokers);
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("auto.offset.reset", "earliest");
			props.put("session.timeout.ms", "30000");
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
			final String inTopic = "txn";
			final String outTopic = "txn-op";
			final StreamsBuilder builder = new StreamsBuilder();

			// builder.stream(inTopic).to(outTopic);

			final KStream<String, String> source = builder.stream(inTopic);

			final KTable<String, Long> counts = source
					.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
					.groupBy((key, value) -> value).count();

			// need to override value serde to Long type
			counts.toStream().to(outTopic, Produced.with(stringSerde, longSerde));
			final KafkaStreams streams = new KafkaStreams(builder.build(), props);
			final CountDownLatch latch = new CountDownLatch(1);

			// attach shutdown handler to catch control-c
			Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
				@Override
				public void run() {
					streams.close();
					latch.countDown();
				}
			});

			try {
				streams.start();
				latch.await();
			} catch (final Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			System.exit(0);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}
