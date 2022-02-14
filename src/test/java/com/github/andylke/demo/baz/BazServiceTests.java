package com.github.andylke.demo.baz;

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
class BazServiceTests {

  @TestConfiguration(proxyBeanMethods = false)
  static class BazServiceTestConfiguration {}

  @Autowired private BazService bazService;

  @Test
  void save() {
    final Baz result = bazService.save("one", "en", Thread.currentThread().getName());
    System.out.println(
        "Saved " + ReflectionToStringBuilder.toString(result, ToStringStyle.NO_CLASS_NAME_STYLE));
  }

  @Test
  void concurrentSaves() throws InterruptedException, ExecutionException {
    final String[] codes = new String[] {"a", "a", "a", "a", "a"};
    final String[] languageCodes = new String[] {"en", "cn", "en", "cn", "en"};
    final List<CompletableFuture<?>> futures = new ArrayList<>();

    final CountDownLatch countDownLatch = new CountDownLatch(codes.length);
    for (int countDown = (int) countDownLatch.getCount(); countDown > 0; countDown--) {
      final String code = codes[countDown - 1];
      final String languageCode = languageCodes[countDown - 1];

      futures.add(
          CompletableFuture.runAsync(
              () -> {
                countDownLatch.countDown();
                try {
                  countDownLatch.await();
                } catch (InterruptedException e) {
                }

                final Baz result =
                    bazService.save(code, languageCode, Thread.currentThread().getName());
                System.out.println(
                    "Saved "
                        + ReflectionToStringBuilder.toString(
                            result, ToStringStyle.NO_CLASS_NAME_STYLE));
              }));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
  }
}
