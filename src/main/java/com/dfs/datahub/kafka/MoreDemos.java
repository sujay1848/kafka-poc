/**
 *
 */
package com.dfs.datahub.kafka;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sanjankar
 *
 */
public class MoreDemos {

	final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(final String[] args) {
		final MoreDemos md = new MoreDemos();
		md.countWords();
	}

	/**
	 * @param brokers
	 */
	private void countWords() {
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		final Properties props = Util.getProperties("word-count-example");
		final String inTopic = "txn";
		final String outTopic = "txn-op";
		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> source = builder.stream(inTopic);

		final KTable<String, Long> counts = source
				.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
				.groupBy((key, value) -> value).count();

		// need to override value serde to Long type
		counts.toStream().to(outTopic, Produced.with(stringSerde, longSerde));
		final KafkaStreams streams = new KafkaStreams(builder.build(), props);

		streams.start();

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
