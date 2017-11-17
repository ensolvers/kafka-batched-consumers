package com.ensolvers.kafka;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class KafkaReaderContext {
  
  private final KafkaReader reader;
  private final BlockingQueue<String> queue;
  private final ExecutorService service;
  
  public KafkaReaderContext(KafkaReader reader, BlockingQueue<String> queue, ExecutorService service) {
    this.reader = reader;
    this.queue = queue;
    this.service = service;
  }
  
  public void start() {
    this.reader.start();
  }
  
  public void shutdown() {
    this.service.shutdown();
    this.reader.shutdown();
  }
  
  protected BlockingQueue<String> getQueue() {
    return queue;
  }
}
