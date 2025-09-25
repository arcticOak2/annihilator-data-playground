package com.annihilator.data.playground.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConnectorConfig {

    private MySQLConnectorConfig mysql;
}
