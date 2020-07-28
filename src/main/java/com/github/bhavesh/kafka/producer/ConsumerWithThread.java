package com.github.bhavesh.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    public static void main(String[] args) {

        new ConsumerWithThread().run();

    }
    public  ConsumerWithThread(){

    }
    public void run(){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "first-group";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("creating the consumer thread");
        Runnable myConsumerThread = new ConsumerThread(latch,topic,bootstrapServers,groupId);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            logger.info("application is closing");
        }
    }

    public class ConsumerThread implements Runnable{
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;

        Properties properties = new Properties();



        public ConsumerThread(CountDownLatch latch, String topic,String bootstrapServers,String groupId){
            this.latch = latch;
            //create consumer configs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe to topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //poll for data
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info("key: " + record.key() + ",value:" + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            }
            catch (WakeupException e){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                // tell our main code we are done with consumer
                latch.countDown();
            }

        }

        public void shutdown(){
            //the wakeup() method will interrupt consumer.poll()
            //it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
