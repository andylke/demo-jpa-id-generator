package com.github.andylke.demo.baz;

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class BazService {

  @Autowired private BazRepository repository;

  @Transactional
  public Baz save(final BigDecimal code, final BigDecimal languageCode, final String text) {
    final Baz baz = new Baz();
    baz.setCode(code);
    baz.setLanguageCode(languageCode);
    baz.setText(text);
    return repository.saveAndFlush(baz);
  }
}
