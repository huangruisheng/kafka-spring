package com.gewara.kafka.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.gewara.kafka.KafkaProducerConnectionFactory;

/**
 * Created by hrs on 14-3-13.
 */
public abstract class KafkaProducerBaseTemplate implements InitializingBean, DisposableBean {

    protected Producer<String, String> producer = null;

    private int processThreads = -1;

    protected ExecutorService executor;

    private KafkaProducerConnectionFactory producerConnectionFactory;


    /**
     * 异步发送
     * @param topic
     * @param key
     * @param value
     */
    public abstract void sendAsync(String topic,String key,String value);
    
    /**
     * 异步发送
     * @param topic
     * @param value
     */
    public abstract void sendAsync(String topic,String value);

    /**
     * 同步发送
     * @param topic
     * @param key
     * @param value
     */
    public abstract void sendSync(String topic,String key,String value);
    
    /**
     * 同步发送
     * @param topic
     * @param value
     */
    public abstract void sendSync(String topic,String value);
    
    public int getProcessThreads() {
        return processThreads;
    }


    public ExecutorService getExecutor() {
        return executor;
    }


    public void setProcessThreads(int processThreads) {
    	 if (processThreads <= 0) {
             throw new IllegalArgumentException("Invalid processThreads value:" + processThreads);
         }
    	 this.processThreads = processThreads;
    }


    public KafkaProducerConnectionFactory getProducerConnectionFactory() {
        return producerConnectionFactory;
    }

    public void setProducerConnectionFactory(KafkaProducerConnectionFactory producerConnectionFactory) {
        this.producerConnectionFactory = producerConnectionFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.processThreads > 0) {
            this.executor = Executors.newFixedThreadPool(this.processThreads);
        }
        if(producer == null){
            producer = new Producer<String, String>(this.producerConnectionFactory.getProducerConfig().getProducerConfig());
        }

    }

    @Override
    public void destroy() throws Exception {
        if(producer!=null){
            producer.close();
        }
        if (this.executor != null) {
            this.executor.shutdown();
            this.executor = null;
        }
    }

}
