package com.annihilator.data.playground.model;

import lombok.Data;

@Data
public class AuthResponse {
  private String accessToken;
  private String refreshToken;
  private String tokenType = "Bearer";
  private int expiresIn; // in seconds
  private User user;

  public AuthResponse(String accessToken, String refreshToken, int expiresIn, User user) {
    this.accessToken = accessToken;
    this.refreshToken = refreshToken;
    this.expiresIn = expiresIn;
    this.user = user;
  }
}
