package com.ensolvers.kafka;

import java.util.concurrent.BlockingQueue;

class SimpleMessageAdaptor implements Runnable {
  
  private final BlockingQueue<String> queue;
  private final MessageListener target;
  
  public SimpleMessageAdaptor(BlockingQueue<String> queue, MessageListener target) {
    this.queue = queue;
    this.target = target;
  }
  
  @Override
  public void run() {
    while (true) {
      try {
        String message = queue.take();
        this.target.onMessage(message);
      } catch (Exception e) {
        //TODO log
      }
    }
  }
  
}
