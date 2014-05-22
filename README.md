kafka-spring
============

kafa spring插件

1、kafka spring api简介

    com.gewara.kafka.KafkaProducerConnectionFactory：生产端连接配置，现支持配置broker的地址，系列化类，ack的次数等参数
    com.gewara.kafka.producer.KafkaProducerTemplate：发送模板类，用于发送kafka信息
    com.gewara.kafka.KafkaConsumerConnectionFactory：消费端连接配置，现支持配置zk相关的地址，自动提交offset的间隔时间
    com.gewara.kafka.consumer.KafkaConsumerListenerContainer：消息监听器容器，用来存放消息监听器，管理监听器，包括监听器注册和初始化等方法
    com.gewara.kafka.consumer.KafkaConsumerBaseListener：开发中最重要的一个抽象类，是监听器的抽象类，继承此类实现onReceiveMessage(String key,String value)和onReceiveMessage(String message)消费消息
    com.gewara.kafka.consumer.KafkaConsumerBaseListenerAdapter：因为KafkaConsumerBaseListener有两个抽象方法，可以用这个类适配

2、配置，代码

    pom.xml

<!-- kafka -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.8.0</artifactId>
            <version>0.8</version>
        </dependency>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.8.0</version>
        </dependency>
        <dependency>
            <groupId>com.yammer.metrics</groupId>
            <artifactId>metrics-core</artifactId>
            <version>2.2.0</version>
            <exclusions>
                <exclusion>
                    <artifactId>slf4j-api</artifactId>
                    <groupId>org.slf4j</groupId>
                </exclusion>
            </exclusions>
        </dependency>

 

    配置信息

<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-3.0.xsd">
   <!--生产端连接配置-->
     <bean id="kafkaProducerConnectionFactory" class="com.gewara.kafka.KafkaProducerConnectionFactory">
        <property name="producerConfig">
            <bean class="com.gewara.kafka.producer.KafkaProducerConfig">
                <property name="brokers" value="192.168.2.183:9092,192.168.2.188:9092"/>
                <property name="serializerClass" value="kafka.serializer.StringEncoder"/>
                <property name="ack" value="1"/>
            </bean>
        </property>
    </bean>
    <!-- 发送模板 -->
    <bean id="kafkaProducerTemplate"  class="com.gewara.kafka.producer.KafkaProducerTemplate">
        <property name="producerConnectionFactory" ref="kafkaProducerConnectionFactory"/>
        <property name="processThreads" value="10" />
        <property name="topics">
            <list>
                <value>topic183</value>
            </list>
        </property>
    </bean>
     
 
    <!--消费端连接配置-->
    <bean id="kafkaConsumerConnectionFactory" class="com.gewara.kafka.KafkaConsumerConnectionFactory">
        <property name="consumerConfig">
            <bean class="com.gewara.kafka.consumer.KafkaConsumerConfig">
                <property name="zkConnect" value="192.168.2.183:2181,192.168.2.108:2181,192.168.2.182:2181"/>
                <property name="zkSessionTimeoutMs" value="12000"/>
                <property name="zkSyncTimeMs" value="200"/>
                <property name="autoCommitIntervalMs" value="1000"/>
            </bean>
        </property>
    </bean>
 
   <!-- 消费监听器 -->
   <bean id="testConsumerListener" class="com.gewara.kafka.TestConsumerListener">
        <property name="processThreads" value="2" /> <!--最好和分区相同-->
        <property name="groupId" value="group1" />
        <property name="topic" value="topic183" />
    </bean>
    <bean id="kafkaConsumerListenerContainer" class="com.gewara.kafka.consumer.KafkaConsumerListenerContainer">
        <property name="consumerConnectionFactory" ref="kafkaConsumerConnectionFactory" />
        <property name="consumerListeners">
           <list>
             <ref bean="testConsumerListener"/>
           </list>
        </property>
    </bean>
</beans>

    发送消息

@Autowired
    private KafkaProducerTemplate producerTemplate;
     
    @RequestMapping("/common/kafka.xhtml")
    @ResponseBody
    public String kafka() throws Exception{
        String key = StringUtil.getRandomString(5);
        String value = "gw-"+StringUtil.getRandomString(10);
        for(int i=0;i<10;i++){
            Time.sleep(100);
            producerTemplate.send("topic183", i+key, value);
            producerTemplate.send("topic183",value)
        }
        return "success";
    }

    接收消息（方式一）

public class TestConsumerListener extends KafkaConsumerBaseListenerAdapter{
     
     
    @Override
    public void onReceiveMessage(String key,String value) {
        //接受消息
    }
    @Override
    public void onReceiveMessage(String message) {
         
    }
 }

    接收消息（方式二）

@Autowired
private ListenerContainer listenerContainer;
 
 
listenerContainer.register(new KafkaConsumerListener() {
            @Override
            public void receiveMessages(String key, String value) {
                 
            }
 
            @Override
            public void receiveMessages(String value){
 
            }
 
            @Override
            public String getTopic() {
                return "topic183";
            }
             
             
            @Override
            public String getGroupId() {
                return "group1";
            }
             
            @Override
            public int getProcessThreads() {
                return 2;
            }
        });

 
