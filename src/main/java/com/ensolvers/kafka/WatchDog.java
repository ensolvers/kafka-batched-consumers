package com.ensolvers.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class WatchDog {
  
  private static Logger logger = LoggerFactory.getLogger(WatchDog.class);
  
  private final WatchDogListener listener;
  private final int duration;
  private final TimeUnit unit;
  private final ExecutorService service;
  private final Semaphore semaphore;
  private volatile Date startTime;
  
  public WatchDog(WatchDogListener theListener, int theDuration, TimeUnit theUnit) {
    this.listener = theListener;
    this.duration = theDuration;
    this.unit = theUnit;
    this.service = Executors.newSingleThreadExecutor();
    this.semaphore = new Semaphore(0);
    this.service.execute(new Runnable() {
      
      @Override
      public void run() {
        while (true) {
          try {
            semaphore.acquire();
            
            Date initial = startTime;
            long totalMilliseconds = unit.toMillis((long) duration);
            TimeUnit.MILLISECONDS.sleep(totalMilliseconds - (new Date().getTime() - startTime.getTime()));
            
            if (startTime.getTime() == initial.getTime()) {
              listener.onWatchDog();
            }
          } catch (Exception e) {
            logger.error("Exception running watchdog", e);
          }
        }
      }
    });
  }
  
  public void start() {
    this.startTime = new Date();
    semaphore.release();
  }
  
}
