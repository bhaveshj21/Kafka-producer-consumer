package com.github.bhavesh.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create Producer Properties
        Properties properties =  new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //create a producer record
        for(int i=0;i<12;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world1" + Integer.toString(i));
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time record is success fully send or an exception thrown
                    if (e == null) {
                        logger.info("receving  callback");
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("error while producing " + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
