package com.gewara.kafka.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Created by hrs on 14-3-13.
 */
public abstract class KafkaConsumerBaseListener  implements KafkaConsumerListener, InitializingBean, DisposableBean {


	private int processThreads = -1;

	protected ExecutorService executor;

	private String groupId;

    private String topic;

	public int getProcessThreads() {
		return processThreads;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setProcessThreads(int processThreads) {
		if (processThreads < 0) {
			throw new IllegalArgumentException("Invalid processThreads value:" + processThreads);
		}
		this.processThreads = processThreads;
	}

	@Override
	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
        if (null == groupId || "".equals(groupId)) {
            throw new IllegalArgumentException("Invalid groupId value:" + groupId);
        }
		this.groupId = groupId;
	}


    @Override
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic){
        if (null == topic || "".equals(topic)) {
            throw new IllegalArgumentException("Invalid topic value:" + topic);
        }
        this.topic = topic;
    }

	public abstract void onReceiveMessage(String key, String message);
	
	public abstract void onReceiveMessage(String message);

	@Override
	public void receiveMessages(String key, String value) {
		onReceiveMessage(key, value);
	}
	
	
	@Override
	public void receiveMessages(String value) {
		onReceiveMessage(value);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.processThreads > 0) {
			this.executor = Executors.newFixedThreadPool(this.processThreads);
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.executor != null) {
			this.executor.shutdown();
			this.executor = null;
		}
	}

}
