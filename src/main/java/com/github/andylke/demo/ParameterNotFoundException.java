package com.github.andylke.demo;

public class ParameterNotFoundException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public ParameterNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParameterNotFoundException(String s) {
    super(s);
  }
}
