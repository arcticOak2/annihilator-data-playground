package com.annihilator.data.playground.model;

import lombok.Data;

@Data
public class LoginRequest {
    private String userId;
    private String email;
    private String password;
}


