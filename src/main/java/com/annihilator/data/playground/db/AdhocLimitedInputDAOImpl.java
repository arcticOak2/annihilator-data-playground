package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.LimitedRunRequest;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AdhocLimitedInputDAOImpl implements AdhocLimitedInputDAO {

  private static final Gson gson = new Gson();

  private final MetaDBConnection metaDBConnection;

  public AdhocLimitedInputDAOImpl(MetaDBConnection metaDBConnection) {
    this.metaDBConnection = metaDBConnection;
  }

  @Override
  public void createAdhocLimitedInput(
      String run_id, String playground_id, LimitedRunRequest request) throws SQLException {

    String sql =
        "INSERT INTO adhoc_limited_input (run_id, playground_id, input_data, created_at) VALUES (?, ?, ?, ?)";

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, run_id);
      ps.setString(2, playground_id);
      ps.setString(3, gson.toJson(request));
      ps.setLong(4, System.currentTimeMillis());

      ps.executeUpdate();
    } catch (SQLException e) {
      throw e;
    }
  }

  @Override
  public LimitedRunRequest getAdhocLimitedInputByRunId(String runId) throws SQLException {

    String sql =
        "SELECT input_data FROM adhoc_limited_input WHERE run_id = ? ORDER BY created_at DESC LIMIT 1";

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, runId);
      var rs = ps.executeQuery();
      if (rs.next()) {
        String inputData = rs.getString("input_data");
        return gson.fromJson(inputData, LimitedRunRequest.class);
      } else {
        return null;
      }
    } catch (SQLException e) {
      throw e;
    }
  }
}
