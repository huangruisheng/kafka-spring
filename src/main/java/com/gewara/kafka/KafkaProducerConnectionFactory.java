package com.gewara.kafka;

import org.springframework.beans.factory.FactoryBean;

import com.gewara.kafka.producer.KafkaProducerConfig;

/**
 * Created by hrs on 14-3-13.
 */
public class KafkaProducerConnectionFactory implements FactoryBean {

    private KafkaProducerConfig producerConfig;

    public void setProducerConfig(KafkaProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
    }

    @Override
    public Object getObject() throws Exception {
        return this;
    }

    @Override
    public Class<?> getObjectType() {
        return KafkaProducerConnectionFactory.class;
    }


    @Override
    public boolean isSingleton() {
        return true;
    }


    public KafkaProducerConfig getProducerConfig() {
        return producerConfig;
    }
}
