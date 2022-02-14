package com.github.andylke.demo.baz;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class BazService {

  @Autowired private BazRepository fooRepository;

  @Transactional
  public Baz save(final String code, final String text) {
    final Baz baz = new Baz();
    baz.setCode(code);
    baz.setText(text);
    return fooRepository.saveAndFlush(baz);
  }
}
