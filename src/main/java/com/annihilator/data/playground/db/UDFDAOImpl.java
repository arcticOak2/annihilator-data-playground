package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UDFDAOImpl implements UDFDAO {

    private final MetaDBConnection metaDBConnection;

    public UDFDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void createUDF(UDF udf) throws SQLException {
        String sql = "INSERT INTO udfs (id, user_id, name, function_name, jar_s3_path, class_name, parameter_types, return_type, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            UUID uuid = UUID.randomUUID();
            udf.setId(uuid.toString());
            udf.setCreatedAt(System.currentTimeMillis());

            ps.setString(1, uuid.toString());
            ps.setString(2, udf.getUserId());
            ps.setString(3, udf.getName());
            ps.setString(4, udf.getFunctionName());
            ps.setString(5, udf.getJarS3Path());
            ps.setString(6, udf.getClassName());
            ps.setString(7, udf.getParameterTypes());  // NEW: parameter types
            ps.setString(8, udf.getReturnType());      // NEW: return type
            ps.setString(9, udf.getDescription());
            ps.setLong(10, udf.getCreatedAt());

            ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public List<UDF> getUDFsByUserId(String userId) throws SQLException {
        String sql = "SELECT id, user_id, name, function_name, jar_s3_path, class_name, parameter_types, return_type, description, created_at FROM udfs WHERE user_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ResultSet rs = ps.executeQuery();
            List<UDF> udfs = new ArrayList<>();

            while (rs.next()) {
                UDF udf = new UDF();
                udf.setId(rs.getString("id"));
                udf.setUserId(rs.getString("user_id"));
                udf.setName(rs.getString("name"));
                udf.setFunctionName(rs.getString("function_name"));
                udf.setJarS3Path(rs.getString("jar_s3_path"));
                udf.setClassName(rs.getString("class_name"));
                udf.setParameterTypes(rs.getString("parameter_types"));  // NEW: parameter types
                udf.setReturnType(rs.getString("return_type"));          // NEW: return type
                udf.setDescription(rs.getString("description"));
                udf.setCreatedAt(rs.getLong("created_at"));

                udfs.add(udf);
            }
            return udfs;
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public UDF getUDFById(String udfId) throws SQLException {
        String sql = "SELECT id, user_id, name, function_name, jar_s3_path, class_name, parameter_types, return_type, description, created_at FROM udfs WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, udfId);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                UDF udf = new UDF();
                udf.setId(rs.getString("id"));
                udf.setUserId(rs.getString("user_id"));
                udf.setName(rs.getString("name"));
                udf.setFunctionName(rs.getString("function_name"));
                udf.setJarS3Path(rs.getString("jar_s3_path"));
                udf.setClassName(rs.getString("class_name"));
                udf.setParameterTypes(rs.getString("parameter_types"));
                udf.setReturnType(rs.getString("return_type"));
                udf.setDescription(rs.getString("description"));
                udf.setCreatedAt(rs.getLong("created_at"));
                return udf;
            }
            return null; // UDF not found
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public void deleteUDFById(String udfId) throws SQLException {

        String sql = "DELETE FROM udfs WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, udfId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }
}



