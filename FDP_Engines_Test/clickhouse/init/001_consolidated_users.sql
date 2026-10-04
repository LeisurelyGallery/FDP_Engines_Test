CREATE DATABASE IF NOT EXISTS fdp;

CREATE TABLE IF NOT EXISTS fdp.consolidated_users
(
    id String,
    first_name String,
    last_name String,
    email String,
    username String,
    phone String,
    zip String,
    gender String,
    test_first_name String,
    test_last_name String,
    test_email String,
    test_username String,
    test_phone String,
    test_zip String,
    test_gender String,
    joined_at DateTime64(3, 'Asia/Kolkata')
)
ENGINE = MergeTree
ORDER BY id;