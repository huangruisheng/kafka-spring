package com.gewara.kafka.producer;

import java.util.Properties;

import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaProducerConfig {

	private String brokers;

	private String serializerClass;

	private String ack;

	public String getBrokers() {
		return brokers;
	}

	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}

	public String getSerializerClass() {
		return serializerClass;
	}

	public void setSerializerClass(String serializerClass) {
		this.serializerClass = serializerClass;
	}

	public String getAck() {
		return ack;
	}

	public void setAck(String ack) {
		this.ack = ack;
	}

	public ProducerConfig getProducerConfig() {
		if (StringUtils.isBlank(brokers)) {
			throw new IllegalArgumentException("Blank brokers");
		}
		if (StringUtils.isBlank(serializerClass)) {
			throw new IllegalArgumentException("Blank serializerClass");
		}
		if (StringUtils.isBlank(ack)) {
			throw new IllegalArgumentException("Blank ack");
		}
		Properties props = new Properties();
		props.put("metadata.broker.list", this.brokers);
		props.put("serializer.class", this.serializerClass);
		props.put("request.required.acks", this.ack);
		return new ProducerConfig(props);
	}
}
