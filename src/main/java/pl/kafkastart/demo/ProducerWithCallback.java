package pl.kafkastart.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

	public static void main(String[] args) {

		//logge

		final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

		//config

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//producer

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		//producer record
		for (int i = 0; i< 10; i++) {
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",	 																			 "helloWorld"+ 																					Integer.toString(i));

			//send data
			kafkaProducer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if (e == null) {
						logger.info("Reciverd new metadada \n" +
								"Topic " + recordMetadata.topic() +
								"Partition " + recordMetadata.partition() +
								"Offset " + recordMetadata.offset() +
								"s " + recordMetadata.timestamp());
					} else {
						logger.error("Error while parsing");

					}
				}

			});
		}
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
