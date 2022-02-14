package com.github.andylke.demo.baz;

import java.io.Serializable;

public class BazId implements Serializable {

  private static final long serialVersionUID = 1L;

  private int id;

  private String code;

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
}
