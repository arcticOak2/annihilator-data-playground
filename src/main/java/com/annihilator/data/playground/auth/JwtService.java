package com.annihilator.data.playground.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtService {

  private static final Logger logger = LoggerFactory.getLogger(JwtService.class);

  private final Algorithm algorithm;
  private final JWTVerifier verifier;
  private final int tokenExpirationMinutes;
  private final int refreshTokenExpirationDays;

  public JwtService(String secretKey, int tokenExpirationMinutes, int refreshTokenExpirationDays) {
    this.algorithm = Algorithm.HMAC256(secretKey);
    this.verifier = JWT.require(algorithm).build();
    this.tokenExpirationMinutes = tokenExpirationMinutes;
    this.refreshTokenExpirationDays = refreshTokenExpirationDays;
  }

  public String createToken(String userId, String username, String email) {
    try {
      Date expiresAt = new Date(System.currentTimeMillis() + (tokenExpirationMinutes * 60 * 1000L));

      return JWT.create()
          .withSubject(userId)
          .withClaim("username", username)
          .withClaim("email", email)
          .withIssuedAt(new Date())
          .withExpiresAt(expiresAt)
          .sign(algorithm);

    } catch (JWTCreationException e) {
      logger.error("Error creating JWT token", e);
      throw new RuntimeException("Failed to create JWT token", e);
    }
  }

  public String createRefreshToken(String userId) {
    try {
      Date expiresAt =
          new Date(
              System.currentTimeMillis() + (refreshTokenExpirationDays * 24 * 60 * 60 * 1000L));

      return JWT.create()
          .withSubject(userId)
          .withClaim("type", "refresh")
          .withIssuedAt(new Date())
          .withExpiresAt(expiresAt)
          .sign(algorithm);

    } catch (JWTCreationException e) {
      logger.error("Error creating refresh token", e);
      throw new RuntimeException("Failed to create refresh token", e);
    }
  }

  public DecodedJWT verifyToken(String token) {
    try {
      return verifier.verify(token);
    } catch (JWTVerificationException e) {
      logger.warn("JWT token verification failed: {}", e.getMessage());
      return null;
    }
  }

  public boolean isTokenValid(String token) {
    return verifyToken(token) != null;
  }

  public String getUserIdFromToken(String token) {
    DecodedJWT decodedJWT = verifyToken(token);
    return decodedJWT != null ? decodedJWT.getSubject() : null;
  }

  public String getUsernameFromToken(String token) {
    DecodedJWT decodedJWT = verifyToken(token);
    return decodedJWT != null ? decodedJWT.getClaim("username").asString() : null;
  }

  public String getEmailFromToken(String token) {
    DecodedJWT decodedJWT = verifyToken(token);
    return decodedJWT != null ? decodedJWT.getClaim("email").asString() : null;
  }

  public boolean isRefreshToken(String token) {
    DecodedJWT decodedJWT = verifyToken(token);
    return decodedJWT != null && "refresh".equals(decodedJWT.getClaim("type").asString());
  }
}
