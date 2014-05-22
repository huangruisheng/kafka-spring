package com.gewara.kafka.consumer;

import java.io.Serializable;

/**
 * Created by hrs on 14-3-17.
 */
public interface ListenerContainer extends Serializable {

	/**
	 * 	
	 * ×¢²áĞÂµÄ¼àÌıÆ÷
	 * @param listener
	 */
    void register(KafkaConsumerListener listener);
}
