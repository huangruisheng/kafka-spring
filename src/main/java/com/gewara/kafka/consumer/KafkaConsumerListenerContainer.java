package com.gewara.kafka.consumer;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.gewara.kafka.KafkaConsumerConnectionFactory;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerListenerContainer implements ListenerContainer, InitializingBean, DisposableBean {


	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerListenerContainer.class);
	
	// 消费监听器列表
	private List<KafkaConsumerListener> consumerListeners = new ArrayList<KafkaConsumerListener>();

	// 消费端配置信息
	private KafkaConsumerConnectionFactory consumerConnectionFactory = null;

	// 消费者链接列表
	private final CopyOnWriteArraySet<ConsumerConnector> consumers = new CopyOnWriteArraySet<ConsumerConnector>();

	@Override
	public void register(final KafkaConsumerListener listener) {
		this.addListener(listener);
		this.consumerListeners.add(listener);

	}

	@Override
	public void afterPropertiesSet() throws Exception {
		for (KafkaConsumerListener listener : this.consumerListeners) {
			this.addListener(listener);
		}
	}

	/**
	 * 添加监听器
	 * 
	 * @param listener
	 */
	private void addListener(KafkaConsumerListener listener) {
		ConsumerConnector consumer = createConnector(listener);
		this.consumers.add(consumer);
		this.poll(listener, consumer);
	}

	/**
	 * 创建连接器
	 * 
	 * @param listener
	 * @return
	 */
	private ConsumerConnector createConnector(KafkaConsumerListener listener) {
		KafkaConsumerConfig kafkaConsumerConfig = this.consumerConnectionFactory.getConsumerConfig();
		Properties properties = kafkaConsumerConfig.getProperties();
		properties.put("group.id", listener.getGroupId());
		return Consumer.createJavaConsumerConnector(kafkaConsumerConfig.getConsumerConfig());
	}

	/**
	 * 获取消息
	 * 
	 * @param listener
	 * @param consumer
	 */
	private void poll(final KafkaConsumerListener listener, ConsumerConnector consumer) {
		int threadCount = listener.getProcessThreads();
		String topic = listener.getTopic();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threadCount);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
		for (final KafkaStream stream : streams) {
			threadPool.submit(new Runnable() {
				@Override
				public void run() {
					ConsumerIterator<byte[], byte[]> it = stream.iterator();
					while (it.hasNext()) {
						try {
							MessageAndMetadata<byte[], byte[]> message = it.next();
							String key = null;
							String value = null;
							byte[] byteKey = message.key();
							if (null != byteKey) {
								key = new String(byteKey, "UTF-8");
							}
							value = new String(message.message(), "UTF-8");
							if (null != key) {
								LOG.debug("kafka poll message key=" + key + ",value=" + value);
								listener.receiveMessages(key, value);
							}else{
								LOG.debug("kafka poll message value=" + value);
								listener.receiveMessages(value);
							}
						} catch (UnsupportedEncodingException e) {
							LOG.debug("kafka fetch message error", e);
						}
					}
				}
			});
		}
	}

	public void setConsumerConnectionFactory(KafkaConsumerConnectionFactory consumerConnectionFactory) {
		this.consumerConnectionFactory = consumerConnectionFactory;
	}

	public KafkaConsumerConnectionFactory getConsumerConnectionFactory() {
		return consumerConnectionFactory;
	}

	public List<KafkaConsumerListener> getConsumerListeners() {
		return consumerListeners;
	}

	public void setConsumerListeners(List<KafkaConsumerListener> consumerListeners) {
		this.consumerListeners = consumerListeners;
	}

	@Override
	public void destroy() throws Exception {
		for (ConsumerConnector consumer : consumers) {
			consumer.shutdown();
			consumer = null;
		}
		this.consumers.clear();
		this.consumerListeners.clear();
	}

}
