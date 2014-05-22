package com.gewara.kafka;

import org.springframework.beans.factory.FactoryBean;

import com.gewara.kafka.consumer.KafkaConsumerConfig;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaConsumerConnectionFactory implements FactoryBean {

    private KafkaConsumerConfig consumerConfig;

    @Override
    public Object getObject() throws Exception {
        return this;
    }

    @Override
    public Class<?> getObjectType() {
        return KafkaConsumerConnectionFactory.class;
    }


    @Override
    public boolean isSingleton() {
        return true;
    }

    public KafkaConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(KafkaConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }
}
