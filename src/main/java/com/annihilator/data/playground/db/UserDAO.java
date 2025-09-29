package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.User;
import java.sql.SQLException;

public interface UserDAO {

  void createUser(User user) throws SQLException;

  void deleteUser(String userId) throws SQLException;

  User getUser(String userId) throws SQLException;

  User getUserByEmail(String email) throws SQLException;
}
