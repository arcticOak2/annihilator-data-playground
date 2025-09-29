package com.annihilator.data.playground.auth;

import java.security.Principal;

public class DataPhantomUser implements Principal {

  private final String userId;
  private final String username;
  private final String email;

  public DataPhantomUser(String userId, String username, String email) {
    this.userId = userId;
    this.username = username;
    this.email = email;
  }

  @Override
  public String getName() {
    return username;
  }

  public String getUserId() {
    return userId;
  }

  public String getUsername() {
    return username;
  }

  public String getEmail() {
    return email;
  }
}
