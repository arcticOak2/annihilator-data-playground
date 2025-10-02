-- ============================
-- Data Phantom Database Schema
-- ============================
-- Complete DDL to recreate database from scratch
-- Updated: September 2025

-- Create the database
CREATE DATABASE IF NOT EXISTS data_phantom;
USE data_phantom;

-- ============================
-- Users Table
-- ============================
CREATE TABLE `users` (
                         `user_id` varchar(100) NOT NULL,
                         `username` varchar(100) NOT NULL,
                         `email` varchar(255) NOT NULL,
                         `password_hash` varchar(255) DEFAULT NULL,
                         PRIMARY KEY (`user_id`),
                         UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Playgrounds Table
-- ============================
CREATE TABLE `playgrounds` (
                               `id` char(36) NOT NULL,
                               `name` varchar(255) DEFAULT NULL,
                               `created_at` bigint(20) NOT NULL,
                               `user_id` varchar(100) NOT NULL,
                               `cron_expression` varchar(255) DEFAULT NULL,
                               `modified_at` bigint(20) DEFAULT NULL,
                               `last_executed_at` bigint(20) DEFAULT NULL,
                               `current_status` varchar(20) DEFAULT 'IDLE',
                               `last_run_status` varchar(20) DEFAULT NULL,
                               `last_run_end_time` bigint(20) DEFAULT NULL,
                               `last_run_success_count` int(11) DEFAULT NULL,
                               `last_run_failure_count` int(11) DEFAULT NULL,
                               `correlation_id` uuid DEFAULT NULL,
                               PRIMARY KEY (`id`),
                               KEY `fk_playground_user` (`user_id`),
                               CONSTRAINT `fk_playground_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Tasks Table
-- ============================
CREATE TABLE `tasks` (
                         `id` char(36) NOT NULL,
                         `name` varchar(255) DEFAULT NULL,
                         `playground_id` char(36) NOT NULL,
                         `parent_id` char(36) DEFAULT NULL,
                         `created_at` bigint(20) NOT NULL,
                         `modified_at` bigint(20) NOT NULL,
                         `type` varchar(50) NOT NULL,
                         `query` mediumtext NOT NULL,
                         `output_location` varchar(500) DEFAULT NULL,
                         `log_path` varchar(500) DEFAULT NULL,
                         `result_preview` text DEFAULT NULL,
                         `task_status` varchar(50) NOT NULL,
                         `last_run_status` varchar(20) DEFAULT NULL,
                         `correlation_id` uuid DEFAULT NULL,
                         `last_correlation_id` uuid DEFAULT NULL,
                         `udf_ids` text DEFAULT NULL,
                         PRIMARY KEY (`id`),
                         KEY `fk_task_playground` (`playground_id`),
                         KEY `fk_task_parent` (`parent_id`),
                         CONSTRAINT `fk_task_parent` FOREIGN KEY (`parent_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE,
                         CONSTRAINT `fk_task_playground` FOREIGN KEY (`playground_id`) REFERENCES `playgrounds` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Playground Run History Table
-- ============================

CREATE TABLE `playground_run_history` (
                                          `run_id` varchar(36) NOT NULL,
                                          `child_id` varchar(36) NOT NULL,
                                          `playground_id` varchar(36) NOT NULL,
                                          `playground_name` varchar(255) DEFAULT NULL,
                                          `parent_id` varchar(36) DEFAULT NULL,
                                          `parent_name` varchar(255) DEFAULT NULL,
                                          `child_name` varchar(255) DEFAULT NULL,
                                          `task_status` varchar(50) DEFAULT NULL,
                                          `task_type` enum('ROOT','CHILD') NOT NULL,
                                          `playground_started_at` timestamp NULL DEFAULT NULL,
                                          `playground_ended_at` timestamp NULL DEFAULT NULL,
                                          `created_at` timestamp NULL DEFAULT current_timestamp(),
                                          PRIMARY KEY (`run_id`,`child_id`),
                                          KEY `child_id` (`child_id`),
                                          KEY `idx_playground_id` (`playground_id`),
                                          KEY `idx_parent_id` (`parent_id`),
                                          KEY `idx_task_status` (`task_status`),
                                          KEY `idx_task_type` (`task_type`),
                                          KEY `idx_playground_started_at` (`playground_started_at`),
                                          KEY `idx_playground_ended_at` (`playground_ended_at`),
                                          KEY `idx_run_playground` (`run_id`,`playground_id`),
                                          KEY `idx_playground_status` (`playground_id`,`task_status`),
                                          KEY `idx_run_status` (`run_id`,`task_status`),
                                          KEY `idx_playground_type` (`playground_id`,`task_type`),
                                          KEY `idx_parent_child` (`parent_id`,`child_id`),
                                          KEY `idx_playground_time_range` (`playground_id`,`playground_started_at`,`playground_ended_at`),
                                          CONSTRAINT `playground_run_history_ibfk_1` FOREIGN KEY (`playground_id`) REFERENCES `playgrounds` (`id`) ON DELETE CASCADE,
                                          CONSTRAINT `playground_run_history_ibfk_2` FOREIGN KEY (`parent_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE,
                                          CONSTRAINT `playground_run_history_ibfk_3` FOREIGN KEY (`child_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Limited ADHOC input recovery Table
-- ============================

CREATE TABLE `adhoc_limited_input` (
                                       `run_id` uuid NOT NULL,
                                       `playground_id` char(36) NOT NULL,
                                       `input_data` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`input_data`)),
                                       `created_at` bigint(20) NOT NULL,
                                       PRIMARY KEY (`run_id`),
                                       KEY `idx_adhoc_limited_input_playground_id` (`playground_id`),
                                       CONSTRAINT `adhoc_limited_input_ibfk_1` FOREIGN KEY (`playground_id`) REFERENCES `playgrounds` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Reconciliation Mappings
-- ============================

CREATE TABLE `reconciliation_mappings` (
                                           `reconciliation_id` uuid NOT NULL,
                                           `playground_id` char(36) NOT NULL,
                                           `left_table_id` char(36) NOT NULL,
                                           `right_table_id` char(36) NOT NULL,
                                           `map` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`map`)),
                                           `created_at` bigint(20) NOT NULL,
                                           `updated_at` bigint(20) NOT NULL,
                                           PRIMARY KEY (`reconciliation_id`),
                                           UNIQUE KEY `unique_reconciliation` (`playground_id`,`left_table_id`,`right_table_id`),
                                           KEY `left_table_id` (`left_table_id`),
                                           KEY `right_table_id` (`right_table_id`),
                                           CONSTRAINT `reconciliation_mappings_ibfk_1` FOREIGN KEY (`playground_id`) REFERENCES `playgrounds` (`id`) ON DELETE CASCADE,
                                           CONSTRAINT `reconciliation_mappings_ibfk_2` FOREIGN KEY (`left_table_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE,
                                           CONSTRAINT `reconciliation_mappings_ibfk_3` FOREIGN KEY (`right_table_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- Reconciliation Results
-- ============================

CREATE TABLE `reconciliation_results` (
                                          `reconciliation_id` uuid NOT NULL,
                                          `execution_timestamp` timestamp NULL DEFAULT current_timestamp(),
                                          `status` enum('SUCCESS','FAILED') NOT NULL,
                                          `left_file_row_count` int(11) NOT NULL DEFAULT 0,
                                          `right_file_row_count` int(11) NOT NULL DEFAULT 0,
                                          `common_row_count` int(11) NOT NULL DEFAULT 0,
                                          `left_file_exclusive_row_count` int(11) NOT NULL DEFAULT 0,
                                          `right_file_exclusive_row_count` int(11) NOT NULL DEFAULT 0,
                                          `sample_common_rows_s3_path` varchar(1000) DEFAULT NULL,
                                          `sample_exclusive_left_rows_s3_path` varchar(1000) DEFAULT NULL,
                                          `sample_exclusive_right_rows_s3_path` varchar(1000) DEFAULT NULL,
                                          `reconciliation_method` varchar(50) DEFAULT 'EXACT_MATCH',
                                          PRIMARY KEY (`reconciliation_id`),
                                          CONSTRAINT `fk_reconciliation_results_reconciliation_id` FOREIGN KEY (`reconciliation_id`) REFERENCES `reconciliation_mappings` (`reconciliation_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ============================
-- UDFs
-- ============================

CREATE TABLE `udfs` (
                        `id` char(36) NOT NULL,
                        `user_id` varchar(100) NOT NULL,
                        `name` varchar(255) NOT NULL,
                        `function_name` varchar(255) NOT NULL,
                        `jar_s3_path` varchar(500) NOT NULL,
                        `class_name` varchar(500) NOT NULL,
                        `description` text DEFAULT NULL,
                        `created_at` bigint(20) NOT NULL,
                        `parameter_types` varchar(500) DEFAULT NULL,
                        `return_type` varchar(100) DEFAULT NULL,
                        PRIMARY KEY (`id`),
                        KEY `fk_udf_user` (`user_id`),
                        KEY `idx_user_function` (`user_id`,`function_name`),
                        CONSTRAINT `fk_udf_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci

-- ============================
-- Notification Destinations
-- ============================

CREATE TABLE `notification_destinations` (
                                             `id` char(36) NOT NULL,
                                             `playground_id` char(36) NOT NULL,
                                             `destination_type` varchar(50) NOT NULL,
                                             `destination` varchar(500) NOT NULL,
                                             `created_at` bigint(20) NOT NULL,
                                             PRIMARY KEY (`id`),
                                             KEY `fk_notification_playground` (`playground_id`),
                                             KEY `idx_playground_type` (`playground_id`,`destination_type`),
                                             CONSTRAINT `fk_notification_playground` FOREIGN KEY (`playground_id`) REFERENCES `playgrounds` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci