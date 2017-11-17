package com.ensolvers.kafka;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class KafkaReaderBuilder {
  
  public static KafkaReaderBuilder readerOn(String topic) {
    return new KafkaReaderBuilder(topic);
  }
  
  private String zookeeper;
  private int zookeeperTimeout;
  private String groupId;
  private String topic;
  private int numberOfThreads;
  private int batchMaxSize;
  private MessageListener singleListener;
  private MessageBatchListener batchListener;
  private int maxWaitingTime;
  private TimeUnit maxWaitingTimeUnit;
  private int consumerThreads;
  
  private KafkaReaderBuilder() {
    this.zookeeper = "localhost:2181";
    this.zookeeperTimeout = 30000;
    this.groupId = UUID.randomUUID().toString();
    this.numberOfThreads = 1;
    this.batchMaxSize = 1;
    this.maxWaitingTime = 3;
    this.consumerThreads = 1;
    this.maxWaitingTimeUnit = TimeUnit.SECONDS;
  }
  
  public KafkaReaderContext build() {
    final BlockingQueue queue;
    
    ExecutorService service = Executors.newFixedThreadPool(this.numberOfThreads);
    KafkaMessageListener kafkaMessageListener;
    
    if (this.batchMaxSize == 1) {
      queue = new LinkedBlockingQueue<String>((this.batchMaxSize * this.numberOfThreads) + 10);
    } else {
      queue = new LinkedBlockingQueue<List<String>>((this.batchMaxSize * this.numberOfThreads) + 10);
    }
    
    if (this.batchMaxSize == 1) {
      for (int i = 0; i < this.numberOfThreads; i++) {
        Runnable job = new SimpleMessageAdaptor(queue, this.singleListener);
        service.execute(job);
      }
      
      kafkaMessageListener = new KafkaMessageListener() {
        
        @Override
        public void onMessage(String message) {
          try {
            queue.put(message);
          } catch (InterruptedException e) {
            // TODO log
          }
        }
      };
    } else {
      kafkaMessageListener = new AccumulatorKafkaMessageListener(
                                                                  this.batchMaxSize,
                                                                  this.numberOfThreads,
                                                                  this.batchListener,
                                                                  this.maxWaitingTime,
                                                                  this.maxWaitingTimeUnit);
    }
    
    KafkaReader reader = new KafkaReader(this.zookeeper, this.zookeeperTimeout, this.groupId, this.consumerThreads, this.topic, kafkaMessageListener);
    
    return new KafkaReaderContext(reader, queue, service);
  }
  
  private KafkaReaderBuilder(String topic) {
    this();
    this.topic = topic;
  }
  
  public KafkaReaderBuilder zookeeper(String zookeeper) {
    this.zookeeper = zookeeper;
    return this;
  }
  
  public KafkaReaderBuilder zookeeperTimeout(int zookeeperTimeout) {
    this.zookeeperTimeout = zookeeperTimeout;
    return this;
  }
  
  public KafkaReaderBuilder groupId(String groupId) {
    this.groupId = groupId;
    return this;
  }
  
  public KafkaReaderBuilder numberOfThreads(int numberOfThreads) {
    if (numberOfThreads < 1) {
      throw new IllegalArgumentException("Number of threads must be > 1");
    }
    
    this.numberOfThreads = numberOfThreads;
    return this;
  }
  
  public KafkaReaderBuilder batchMaxSize(int batchMaxSize) {
    if (batchMaxSize < 1) {
      throw new IllegalArgumentException("Max batch size must be > 1");
    }
    
    this.batchMaxSize = batchMaxSize;
    return this;
  }
  
  public KafkaReaderBuilder listener(MessageListener listener) {
    this.singleListener = listener;
    return this;
  }
  
  public KafkaReaderBuilder listener(MessageBatchListener listener) {
    this.batchListener = listener;
    return this;
  }
  
  public KafkaReaderBuilder consumerThreads(int consumerThreads) {
    this.consumerThreads = consumerThreads;
    return this;
  }
  
  public KafkaReaderBuilder batchWaitAtMost(int maxWaitingTime, TimeUnit maxWaitingTimeUnit) {
    this.maxWaitingTime = maxWaitingTime;
    this.maxWaitingTimeUnit = maxWaitingTimeUnit;
    return this;
  }
}
