package com.ensolvers.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class AccumulatorKafkaMessageListener implements KafkaMessageListener, WatchDogListener {
  
  private static Logger logger = LoggerFactory.getLogger(AccumulatorKafkaMessageListener.class);
  
  private final List<String> messages;
  private final BlockingQueue<List<String>> queue;
  private final int batchSize;
  private final int threadProcessingSize;
  private final WatchDog watchDog;
  private final ExecutorService processingService;
  private final MessageBatchListener batchListener;
  
  public AccumulatorKafkaMessageListener(
                                          int batchSize,
                                          int threadProcessingSize,
                                          MessageBatchListener theBatchListener,
                                          int maxWaitingTime,
                                          TimeUnit maxWaitingTimeUnit) {
    
    this.messages = new ArrayList<String>();
    
    this.batchListener = theBatchListener;
    this.batchSize = batchSize;
    this.threadProcessingSize = threadProcessingSize;
    this.queue = new LinkedBlockingQueue<List<String>>((this.batchSize * this.threadProcessingSize) + 10);
    this.watchDog = new WatchDog(this, maxWaitingTime, maxWaitingTimeUnit);
    
    this.processingService = Executors.newFixedThreadPool(this.threadProcessingSize);
    
    for (int i = 0; i < this.threadProcessingSize; i++) {
      this.processingService.execute(new Runnable() {
        @Override
        public void run() {
          while (true) {
            try {
              List<String> messages = queue.take();
              logger.info("Pending in batch queue: " + queue.size());
              batchListener.onMessageBatch(messages);
            } catch (Exception e) {
              logger.error("Exception getting data from queue", e);
            }
          }
        }
      });
    }
  }
  
  @Override
  public synchronized void onMessage(String message) {
    if (messages.isEmpty()) {
      watchDog.start();
    }
    
    messages.add(message);
    
    if (messages.size() == batchSize) {
      this.putOnQueue();
    }
  }
  
  private void putOnQueue() {
    if (!this.messages.isEmpty()) {
      try {
        queue.put(new ArrayList<String>(messages));
        messages.clear();
      } catch (InterruptedException e) {
        logger.error("Error putting messages in the queue", e);
      }
    }
  }
  
  public synchronized void onWatchDog() {
    this.putOnQueue();
  }
}
