package com.annihilator.data.playground.model;

import lombok.Data;

@Data
public class PasswordResetRequest {
  private String email;
  private String newPassword;
  private String resetToken;
}
