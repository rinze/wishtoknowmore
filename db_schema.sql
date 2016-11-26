DROP TABLE IF EXISTS monitor;
CREATE TABLE monitor (
    request_timestamp BIGINT,
    thread_creation_timestamp BIGINT,
    request_comment_id VARCHAR(20),
    request_user VARCHAR(20),
    request_reply VARCHAR(20),
    request_thread VARCHAR(20),
    PRIMARY KEY (request_thread(20))
);

DROP TABLE IF EXISTS processed;
CREATE TABLE processed (
    processing_timestamp BIGINT,
    post_id VARCHAR(20),
    request_thread VARCHAR(20),
    PRIMARY KEY (request_thread(20))
);

DROP TABLE IF EXISTS last_comment;
CREATE TABLE last_comment (
    last_comment VARCHAR(20) 
);

INSERT INTO last_comment (last_comment) VALUES ('000000');
