package com.capitolssg.kafka.sinks;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaSink implements ILogSink {

	private Producer<String, String> producer;
	private String topic;
	
	public KafkaSink(String brokerList, String topic, String serializer, String partitioner, boolean synchronous) {
		this.topic = topic;
		
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("serializer.class", serializer);
		props.put("partitioner.class", partitioner);
		props.put("request.required.acks", synchronous ? "1" : "0");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
	}
	
	@Override
	public void send(String key, String message) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, message);
	    producer.send(data);
	}

	@Override
	public void close() {
		producer.close();
		
	}

}
