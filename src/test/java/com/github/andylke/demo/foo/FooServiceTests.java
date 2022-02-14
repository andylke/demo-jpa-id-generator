package com.github.andylke.demo.foo;

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
class FooServiceTests {

  @TestConfiguration(proxyBeanMethods = false)
  static class FooServiceTestConfiguration {}

  @Autowired private FooService fooService;

  @Test
  void save() {
    final Foo result = fooService.save("one");
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

                final Foo result = fooService.save(Thread.currentThread().getName());
                System.out.println(
                    "Saved "
                        + ReflectionToStringBuilder.toString(
                            result, ToStringStyle.NO_CLASS_NAME_STYLE));
              }));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
  }
}
