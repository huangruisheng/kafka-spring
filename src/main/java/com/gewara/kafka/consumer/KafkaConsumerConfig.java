package com.gewara.kafka.consumer;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;

import org.apache.commons.lang.StringUtils;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerConfig {

	private String zkConnect;

	private String zkSessionTimeoutMs;

	private String zkSyncTimeMs;

	private String autoCommitIntervalMs;

	private Properties properties;

	public KafkaConsumerConfig() {
		properties = new Properties();
	}

	public String getZkConnect() {
		return zkConnect;
	}

	public void setZkConnect(String zkConnect) {
		this.zkConnect = zkConnect;
	}

	public String getZkSessionTimeoutMs() {
		return zkSessionTimeoutMs;
	}

	public void setZkSessionTimeoutMs(String zkSessionTimeoutMs) {
		this.zkSessionTimeoutMs = zkSessionTimeoutMs;
	}

	public String getZkSyncTimeMs() {
		return zkSyncTimeMs;
	}

	public void setZkSyncTimeMs(String zkSyncTimeMs) {
		this.zkSyncTimeMs = zkSyncTimeMs;
	}

	public String getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

	public ConsumerConfig getConsumerConfig() {
		if (StringUtils.isBlank(zkConnect)) {
			throw new IllegalArgumentException("Blank zkConnect");
		}
		if (StringUtils.isNotBlank(zkSessionTimeoutMs)) {
			properties.put("zookeeper.session.timeout.ms", this.zkSessionTimeoutMs);
		}
		if (StringUtils.isNotBlank(zkSyncTimeMs)) {
			properties.put("zookeeper.sync.time.ms", this.zkSyncTimeMs);
		}
		if (StringUtils.isNotBlank(autoCommitIntervalMs)) {
			properties.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
		}
		
		properties.put("zookeeper.connect", this.zkConnect);
		return new ConsumerConfig(properties);
	}

	public Properties getProperties() {
		return properties;
	}
}
