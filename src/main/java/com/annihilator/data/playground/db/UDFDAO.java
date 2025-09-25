package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.UDF;

import java.sql.SQLException;
import java.util.List;

public interface UDFDAO {

    void createUDF(UDF udf) throws SQLException;

    List<UDF> getUDFsByUserId(String userId) throws SQLException;

    UDF getUDFById(String udfId) throws SQLException;

    void deleteUDFById(String udfId) throws SQLException;
}




