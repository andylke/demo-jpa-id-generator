package com.github.andylke.demo.foo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class FooService {

  @Autowired private FooRepository fooRepository;

  @Transactional
  public Foo save(final String text) {
    final Foo foo = new Foo();
    foo.setText(text);
    return fooRepository.saveAndFlush(foo);
  }
}
