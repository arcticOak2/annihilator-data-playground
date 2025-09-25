package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.User;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class UserDAOImpl implements UserDAO {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(UserDAOImpl.class);

    private MetaDBConnection metaDBConnection;

    public UserDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void createUser(User user) throws SQLException {
        String sql = "INSERT INTO users (user_id, username, email, password_hash) VALUES (?, ?, ?, ?)";

        User fetchedUser = getUser(user.getUserId());

        if (Objects.nonNull(fetchedUser)) {
            logger.info("User with ID {} already exists. Skipping creation.", user.getUserId());
            return;
        }

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, user.getUserId());
            ps.setString(2, user.getUsername());
            ps.setString(3, user.getEmail());
            ps.setString(4, user.getPasswordHash());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public void deleteUser(String userId) throws SQLException {
        String sql = "DELETE FROM users WHERE user_id = ?";
        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public User getUser(String userId) throws SQLException {

        String sql = "SELECT user_id, username, email, password_hash FROM users WHERE user_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    User user = new User();
                    user.setUserId(rs.getString("user_id"));
                    user.setUsername(rs.getString("username"));
                    user.setEmail(rs.getString("email"));
                    user.setPasswordHash(rs.getString("password_hash"));

                    return user;
                }
            }

            return null;
        } catch (SQLException e) {
            throw e;
        }
    }
    
    @Override
    public User getUserByEmail(String email) throws SQLException {
        String sql = "SELECT user_id, username, email, password_hash FROM users WHERE email = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    User user = new User();
                    user.setUserId(rs.getString("user_id"));
                    user.setUsername(rs.getString("username"));
                    user.setEmail(rs.getString("email"));
                    user.setPasswordHash(rs.getString("password_hash"));

                    return user;
                }
            }

            return null;
        } catch (SQLException e) {
            throw e;
        }
    }
}

