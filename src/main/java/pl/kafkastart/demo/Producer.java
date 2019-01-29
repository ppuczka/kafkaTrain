package pl.kafkastart.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

	public static void main(String[] args) {

		//config

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//producer

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		//producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic" , "helwo 																					world ");

		//send data
		kafkaProducer.send(record);
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
