package com.ensolvers.kafka;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReader {
  private static Logger logger = LoggerFactory.getLogger(KafkaReader.class);
  private final String zookeeper;
  private final int zookeeperTimeout;
  private final String groupId;
  private final ArrayList<Consumer<String, String>> consumers;
  
  @SuppressWarnings("rawtypes")
  private static class EventConsumer implements Runnable {
    
    private Consumer consumer;
    private KafkaReader reader;
    
    public EventConsumer(
                          Consumer consumer,
                          KafkaReader reader) {
      
      this.consumer = consumer;
      this.reader = reader;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      while (true) {
        final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
        Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator();
        while (it.hasNext()) {
          String message = new String(it.next().value());
          try {
            this.reader.listener.onMessage(message);
          } catch (Exception e) {
            logger.error("Error when processing element: " + message + ". CONTINUE WITH NEXT ONE", e);
          }
        }
      }
      
    }
  }
  
  private final ExecutorService service;
  private final String topic;
  private final KafkaMessageListener listener;
  private final int threadPoolSize;
  
  public KafkaReader(
                      String zookeeper,
                      int zookeeperTimeout,
                      String groupId,
                      int threadPoolSize,
                      String topic,
                      KafkaMessageListener listener) {
    
    this.zookeeper = zookeeper;
    this.zookeeperTimeout = zookeeperTimeout;
    this.groupId = groupId;
    this.consumers = new ArrayList<>();
    this.topic = topic;
    this.listener = listener;
    this.threadPoolSize = threadPoolSize;
    this.service = Executors.newFixedThreadPool(threadPoolSize);
  }
  
  public void start() {
    this.run(this.threadPoolSize);
  }
  
  @SuppressWarnings("rawtypes")
  private void run(int threadPoolSize) {
    String topic = this.topic;
    
    for (int threadNumber = 0; threadNumber < threadPoolSize; threadNumber++) {
      Consumer<String, String> consumer = new KafkaConsumer<>(createConsumerConfig(this.zookeeper, this.zookeeperTimeout, this.groupId));
      consumer.subscribe(Collections.singleton(topic));
      this.consumers.add(consumer);
      this.service.execute(this.getEventConsumer(consumer, threadNumber));
    }
  }
  
  @SuppressWarnings("rawtypes")
  protected Runnable getEventConsumer(Consumer consumer, int threadNumber) {
    return new EventConsumer(consumer, this);
  }
  
  public void shutdown() {
    for (Consumer consumer : this.consumers) {
      consumer.close(5, TimeUnit.SECONDS);
    }
    if (service != null) service.shutdown();
    
    try {
      if (!service.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      logger.error("Interrupted during shutdown, exiting uncleanly", e);
    }
  }
  
  private Properties createConsumerConfig(String zookeeper, Integer zookeeperTimeout, String groupId) {
    Properties config = new Properties();
    
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, zookeeper);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, zookeeperTimeout.toString());
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, zookeeperTimeout.toString());
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    
    return config;
  }
}
