package com.github.andylke.demo;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import com.github.andylke.demo.foo.Foo;
import com.github.andylke.demo.foo.FooService;

@SpringBootApplication
public class DemoApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }

  @Autowired private FooService fooService;

  @EventListener({ApplicationReadyEvent.class})
  void ready() {
    final CountDownLatch countDownLatch = new CountDownLatch(10);
    for (int countDown = (int) countDownLatch.getCount(); countDown > 0; countDown--) {
      new Thread(
              () -> {
                countDownLatch.countDown();
                try {
                  countDownLatch.await();
                } catch (InterruptedException e) {
                }

                final Foo result = fooService.save(Thread.currentThread().getName());
                LOGGER.info("Saved id={}, text={}", result.getId(), result.getText());
              })
          .start();
    }
  }
}
