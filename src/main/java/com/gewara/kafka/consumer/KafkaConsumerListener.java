package com.gewara.kafka.consumer;


/**
 * Created by hrs on 14-3-13.
 */
public interface KafkaConsumerListener {

	/**
	 * 接受消息
	 * @param value
	 */
    void receiveMessages(String key,String value);
    
    /**
     * 接受消息
     * @param value
     */
    void receiveMessages(String value);
    
    /**
     * 线程数
     * @return
     */
    int getProcessThreads();

    /**
     * 监听器所在消费组
     * @return
     */
    String getGroupId();

    /**
     * 主题名
     * @return
     */
    String getTopic();

	
    
    
    




}
