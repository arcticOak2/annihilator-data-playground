package com.annihilator.data.playground.model;

import lombok.Data;

@Data
public class User {

  private String userId;

  private String username;

  private String email;

  private String passwordHash;
}
