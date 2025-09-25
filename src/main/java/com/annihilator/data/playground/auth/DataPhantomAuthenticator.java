package com.annihilator.data.playground.auth;

import io.dropwizard.auth.Authenticator;

import java.util.Optional;

public class DataPhantomAuthenticator implements Authenticator<String, DataPhantomUser> {

    private final JwtService jwtService;

    public DataPhantomAuthenticator(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Override
    public Optional<DataPhantomUser> authenticate(String token) {
        try {
            if (jwtService.isTokenValid(token)) {
                String userId = jwtService.getUserIdFromToken(token);
                String username = jwtService.getUsernameFromToken(token);
                String email = jwtService.getEmailFromToken(token);
                
                if (userId != null && username != null && email != null) {
                    return Optional.of(new DataPhantomUser(userId, username, email));
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
