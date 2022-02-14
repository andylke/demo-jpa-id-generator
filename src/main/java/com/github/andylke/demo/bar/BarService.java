package com.github.andylke.demo.bar;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class BarService {

  @Autowired private BarRepository barRepository;

  @Transactional
  public Bar save(final String text) {
    final Bar bar = new Bar();
    bar.setText(text);
    return barRepository.saveAndFlush(bar);
  }
}
