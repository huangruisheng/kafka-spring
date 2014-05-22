package com.gewara.kafka.producer;

import java.util.List;

import kafka.producer.KeyedMessage;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaProducerTemplate extends KafkaProducerBaseTemplate {

    private List<String> topics;

    public KafkaProducerTemplate() {
        super();
    }
    
    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
    
	@Override
	public void sendAsync(String topic, String value) {
		if(!topics.contains(topic)){
    		throw new IllegalArgumentException("Invalid topic value:" + topic);
    	}
        final KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,value);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                producer.send(data);
            }
        });	
	}
   
    @Override
    public void sendAsync(String topic,String key,String value){
    	if(!topics.contains(topic)){
    		throw new IllegalArgumentException("Invalid topic value:" + topic);
    	}
        final KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
        executor.submit(new Runnable() {
            @Override
            public void run() {
                producer.send(data);
            }
        });
    }

	@Override
	public void sendSync(String topic, String key, String value) {
		if(!topics.contains(topic)){
    		throw new IllegalArgumentException("Invalid topic value:" + topic);
    	}
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
		producer.send(data);
	}

	@Override
	public void sendSync(String topic, String value) {
		if(!topics.contains(topic)){
    		throw new IllegalArgumentException("Invalid topic value:" + topic);
    	}
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,value);
        producer.send(data);
	}




}
