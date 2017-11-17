package com.ensolvers.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaWriter {
  
  private final KafkaProducer<String, String> producer;
  protected ExecutorService asyncService;
  private long delay;
  private final String topic;
  private static Logger log = LoggerFactory.getLogger(KafkaWriter.class);
  
  protected KafkaWriter(String producersUrl, String topic) {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producersUrl);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.ACKS_CONFIG, "1");
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    
    this.producer = new KafkaProducer<String, String>(config);
    this.asyncService = Executors.newFixedThreadPool(5);
    this.delay = 0;
    this.topic = topic;
  }
  
  public void write(final String key, final String value) {
    this.asyncService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          if(delay != 0) {
            Thread.sleep(delay);
          }
          ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
          producer.send(record);
        } catch (Exception e) {
          log.error("Excpetion ocurred when writing to Kafka", e);
        }
      }
    });
  }
  
  public void close() {
    this.producer.close();
  }
  
  public long getDelay() {
    return delay;
  }
  
  public void setDelay(long delay) {
    this.delay = delay;
  }
}
