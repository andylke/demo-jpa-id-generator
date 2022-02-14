package com.github.andylke.demo.baz;

import java.io.Serializable;

public class BazId implements Serializable {

  private static final long serialVersionUID = 1L;

  private int id;

  private String code;

  private String languageCode;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getLanguageCode() {
    return languageCode;
  }

  public void setLanguageCode(String languageCode) {
    this.languageCode = languageCode;
  }
}
