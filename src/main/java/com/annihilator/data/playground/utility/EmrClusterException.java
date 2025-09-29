package com.annihilator.data.playground.utility;

public class EmrClusterException extends RuntimeException {

  public EmrClusterException(String message) {
    super(message);
  }

  public EmrClusterException(String message, Throwable cause) {
    super(message, cause);
  }
}
