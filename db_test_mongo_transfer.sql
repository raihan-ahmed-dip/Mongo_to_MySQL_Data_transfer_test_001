CREATE DATABASE IF NOT EXISTS db_test_mongo_transfer CHARACTER SET utf8mb4;

USE db_test_mongo_transfer;

 CREATE TABLE IF NOT EXISTS ecook_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sn VARCHAR(255),
        rts DATETIME,
        CDT INT,
        val FLOAT);