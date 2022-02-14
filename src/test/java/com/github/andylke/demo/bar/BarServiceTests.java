package com.github.andylke.demo.bar;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;

@SpringBootTest
public class BarServiceTests {

  @TestConfiguration(proxyBeanMethods = false)
  static class BarServiceTestConfiguration {}

  @Autowired private BarService barService;

  @Test
  void save() {
    final Bar result = barService.save(Thread.currentThread().getName());
    System.out.println(
        "Saved " + ReflectionToStringBuilder.toString(result, ToStringStyle.NO_CLASS_NAME_STYLE));
  }

  @Test
  void concurrentSaves() throws InterruptedException, ExecutionException {
    final List<CompletableFuture<?>> futures = new ArrayList<>();

    final CountDownLatch countDownLatch = new CountDownLatch(5);
    for (int countDown = (int) countDownLatch.getCount(); countDown > 0; countDown--) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                countDownLatch.countDown();
                try {
                  countDownLatch.await();
                } catch (InterruptedException e) {
                }

                final Bar result = barService.save(Thread.currentThread().getName());
                System.out.println(
                    "Saved "
                        + ReflectionToStringBuilder.toString(
                            result, ToStringStyle.NO_CLASS_NAME_STYLE));
              }));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
  }
}
