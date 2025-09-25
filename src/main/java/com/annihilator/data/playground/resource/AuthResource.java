package com.annihilator.data.playground.resource;

import com.annihilator.data.playground.auth.DataPhantomUser;
import com.annihilator.data.playground.auth.JwtService;
import com.annihilator.data.playground.auth.PasswordUtil;
import com.annihilator.data.playground.db.UserDAO;
import com.annihilator.data.playground.model.*;
import java.util.concurrent.ConcurrentHashMap;

import io.dropwizard.auth.Auth;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Path("/auth")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AuthResource {

    private static final Logger logger = LoggerFactory.getLogger(AuthResource.class);

    private final UserDAO userDAO;
    private final JwtService jwtService;
    
    private final Map<String, ResetTokenInfo> resetTokens = new ConcurrentHashMap<>();

    public AuthResource(UserDAO userDAO, JwtService jwtService) {
        this.userDAO = userDAO;
        this.jwtService = jwtService;
    }
    
    private static class ResetTokenInfo {
        private final String email;
        private final long expiryTime;
        
        public ResetTokenInfo(String email, long expiryTime) {
            this.email = email;
            this.expiryTime = expiryTime;
        }
        
        public String getEmail() { return email; }
        public long getExpiryTime() { return expiryTime; }
        public boolean isExpired() { return System.currentTimeMillis() > expiryTime; }
    }

    @POST
    @Path("/register")
    public Response register(RegisterRequest request) {
        try {
            // Validate request
            if (request.getUserId() == null || request.getUserId().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "User ID is required"))
                        .build();
            }
            if (request.getUsername() == null || request.getUsername().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Username is required"))
                        .build();
            }
            if (request.getEmail() == null || request.getEmail().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Email is required"))
                        .build();
            }
            if (request.getPassword() == null || request.getPassword().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Password is required"))
                        .build();
            }

            // Check if user already exists
            User existingUser = userDAO.getUser(request.getUserId());
            if (existingUser != null) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(Map.of("error", "User with this ID already exists"))
                        .build();
            }

            User existingUserByEmail = userDAO.getUserByEmail(request.getEmail());
            if (existingUserByEmail != null) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(Map.of("error", "User with this email already exists"))
                        .build();
            }

            // Create new user
            User user = new User();
            user.setUserId(request.getUserId());
            user.setUsername(request.getUsername());
            user.setEmail(request.getEmail());
            user.setPasswordHash(PasswordUtil.hashPassword(request.getPassword()));

            userDAO.createUser(user);

            // Generate tokens
            String accessToken = jwtService.createToken(user.getUserId(), user.getUsername(), user.getEmail());
            String refreshToken = jwtService.createRefreshToken(user.getUserId());

            // Create response
            AuthResponse response = new AuthResponse(
                    accessToken,
                    refreshToken,
                    60 * 60, // 1 hour in seconds
                    user
            );

            return Response.status(Response.Status.CREATED)
                    .entity(response)
                    .build();

        } catch (SQLException e) {
            logger.error("Error during user registration", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to register user"))
                    .build();
        }
    }

    @POST
    @Path("/login")
    public Response login(LoginRequest request) {
        try {
            // Validate request
            if ((request.getUserId() == null || request.getUserId().trim().isEmpty()) && 
                (request.getEmail() == null || request.getEmail().trim().isEmpty())) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Either userId or email is required"))
                        .build();
            }
            if (request.getPassword() == null || request.getPassword().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Password is required"))
                        .build();
            }

            // Find user by userId or email
            User user = null;
            if (request.getUserId() != null && !request.getUserId().trim().isEmpty()) {
                // Login with userId
                user = userDAO.getUser(request.getUserId());
            } else {
                // Login with email
                user = userDAO.getUserByEmail(request.getEmail());
            }

            if (user == null) {
                return Response.status(Response.Status.UNAUTHORIZED)
                        .entity(Map.of("error", "Invalid credentials"))
                        .build();
            }

            // Verify password
            if (!PasswordUtil.verifyPassword(request.getPassword(), user.getPasswordHash())) {
                return Response.status(Response.Status.UNAUTHORIZED)
                        .entity(Map.of("error", "Invalid credentials"))
                        .build();
            }

            // Generate tokens
            String accessToken = jwtService.createToken(user.getUserId(), user.getUsername(), user.getEmail());
            String refreshToken = jwtService.createRefreshToken(user.getUserId());

            // Create response
            AuthResponse response = new AuthResponse(
                    accessToken,
                    refreshToken,
                    60 * 60, // 1 hour in seconds
                    user
            );

            return Response.ok()
                    .entity(response)
                    .build();

        } catch (SQLException e) {
            logger.error("Error during user login", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to login user"))
                    .build();
        }
    }

    @POST
    @Path("/refresh")
    public Response refreshToken(@QueryParam("refresh_token") String refreshToken) {
        try {
            if (refreshToken == null || refreshToken.trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Refresh token is required"))
                        .build();
            }

            // Verify refresh token
            if (!jwtService.isTokenValid(refreshToken) || !jwtService.isRefreshToken(refreshToken)) {
                return Response.status(Response.Status.UNAUTHORIZED)
                        .entity(Map.of("error", "Invalid refresh token"))
                        .build();
            }

            // Get user ID from refresh token
            String userId = jwtService.getUserIdFromToken(refreshToken);
            if (userId == null) {
                return Response.status(Response.Status.UNAUTHORIZED)
                        .entity(Map.of("error", "Invalid refresh token"))
                        .build();
            }

            // Get user details
            User user = userDAO.getUser(userId);
            if (user == null) {
                return Response.status(Response.Status.UNAUTHORIZED)
                        .entity(Map.of("error", "User not found"))
                        .build();
            }

            // Generate new tokens
            String newAccessToken = jwtService.createToken(user.getUserId(), user.getUsername(), user.getEmail());
            String newRefreshToken = jwtService.createRefreshToken(user.getUserId());

            // Create response
            AuthResponse response = new AuthResponse(
                    newAccessToken,
                    newRefreshToken,
                    60 * 60, // 1 hour in seconds
                    user
            );

            return Response.ok()
                    .entity(response)
                    .build();

        } catch (SQLException e) {
            logger.error("Error during token refresh", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to refresh token"))
                    .build();
        }
    }

    @POST
    @Path("/forgot-password")
    public Response forgotPassword(@QueryParam("email") String email) {
        try {
            // Validate email
            if (email == null || email.trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Email is required"))
                        .build();
            }

            // Check if user exists
            User user = userDAO.getUserByEmail(email);
            if (user == null) {
                // Don't reveal if user exists or not for security
                return Response.ok()
                        .entity(Map.of("message", "If the email exists, a password reset link has been sent"))
                        .build();
            }

            // Generate reset token (valid for 1 hour)
            String resetToken = UUID.randomUUID().toString();
            long expiryTime = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
            
            // Store reset token
            resetTokens.put(resetToken, new ResetTokenInfo(email, expiryTime));
            
            // Clean up expired tokens
            cleanupExpiredTokens();

            // For development: return token in response
            // In production, send email with reset link instead
            logger.info("Password reset token for {}: {}", email, resetToken);

            return Response.ok()
                    .entity(Map.of(
                        "message", "Password reset token generated",
                        "resetToken", resetToken,
                        "expiresIn", "1 hour",
                        "warning", "This is for development only - implement email service for production"
                    ))
                    .build();

        } catch (SQLException e) {
            logger.error("Error during forgot password", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to process password reset request"))
                    .build();
        }
    }

    @POST
    @Path("/reset-password")
    public Response resetPassword(PasswordResetRequest request) {
        try {
            // Validate request
            if (request.getResetToken() == null || request.getResetToken().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Reset token is required"))
                        .build();
            }
            if (request.getNewPassword() == null || request.getNewPassword().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "New password is required"))
                        .build();
            }

            // Validate password strength (optional)
            if (request.getNewPassword().length() < 6) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Password must be at least 6 characters long"))
                        .build();
            }

            // Check if reset token exists and is valid
            ResetTokenInfo tokenInfo = resetTokens.get(request.getResetToken());
            if (tokenInfo == null || tokenInfo.isExpired()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid or expired reset token"))
                        .build();
            }

            // Get user by email from token
            User user = userDAO.getUserByEmail(tokenInfo.getEmail());
            if (user == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "User not found"))
                        .build();
            }

            // Update password
            user.setPasswordHash(PasswordUtil.hashPassword(request.getNewPassword()));
            
            // Update user in database
            userDAO.createUser(user); // This will update existing user due to the logic in createUser

            // Remove used reset token
            resetTokens.remove(request.getResetToken());

            return Response.ok()
                    .entity(Map.of("message", "Password has been reset successfully"))
                    .build();

        } catch (SQLException e) {
            logger.error("Error during password reset", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to reset password"))
                    .build();
        }
    }

    @POST
    @Path("/change-password")
    public Response changePassword(PasswordResetRequest request) {
        try {
            // Validate request
            if (request.getEmail() == null || request.getEmail().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Email is required"))
                        .build();
            }
            if (request.getNewPassword() == null || request.getNewPassword().trim().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "New password is required"))
                        .build();
            }

            // Validate password strength (optional)
            if (request.getNewPassword().length() < 6) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Password must be at least 6 characters long"))
                        .build();
            }

            // Get user by email
            User user = userDAO.getUserByEmail(request.getEmail());
            if (user == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "User not found"))
                        .build();
            }

            // Update password
            user.setPasswordHash(PasswordUtil.hashPassword(request.getNewPassword()));
            
            // Update user in database
            userDAO.createUser(user); // This will update existing user due to the logic in createUser

            return Response.ok()
                    .entity(Map.of("message", "Password has been changed successfully"))
                    .build();

        } catch (SQLException e) {
            logger.error("Error during password change", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to change password"))
                    .build();
        }
    }

    private void cleanupExpiredTokens() {
        resetTokens.entrySet().removeIf(entry -> entry.getValue().isExpired());
    }
}

