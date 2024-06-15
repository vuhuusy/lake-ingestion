-- Description: This script is used to create the database and tables for the project

create datababse if not exists `sales`;

create table transactions (
    id int AUTO_INCREMENT PRIMARY key,
    created_date varchar(50),
    created_time varchar(50),
    uuid varchar(100),
    filename varchar(100),
    path varchar(255)
);

create table staging (
    id int PRIMARY key,
    created_date varchar(50),
    created_time varchar(50),
    uuid varchar(100),
    filename varchar(100),
    path varchar(255)
);

create table process_log (
    processed_date varchar(50), 
    filename varchar(255),
    uuid varchar(255),
    executesql_row_count int,
    record_count int
);

create table processing_stats (
    processed_date date,
    processed_row_count int,
    transactions_row_count int,
    row_difference int AS (processed_row_count - transactions_row_count) STORED,
    data_loss_rate decimal(5,2) AS ((transactions_row_count - processed_row_count) / transactions_row_count * 100) STORED,
    row_difference_from_yesterday int,
    percent_difference_from_yesterday decimal(5,2)
);


-- Create trigger to update row_difference_from_yesterday and percent_difference_from_yesterday

DELIMITER //

CREATE TRIGGER update_yesterday_difference
AFTER INSERT ON processing_stats
FOR EACH ROW
BEGIN
    DECLARE yesterday_processed_row_count INT;

    -- Lấy giá trị processed_row_count của ngày hôm qua
    SELECT processed_row_count INTO yesterday_processed_row_count
    FROM processing_stats
    WHERE processed_date = DATE_SUB(NEW.processed_date, INTERVAL 1 DAY)
    LIMIT 1;

    -- Cập nhật các trường row_difference_from_yesterday và percent_difference_from_yesterday cho bản ghi mới
    IF yesterday_processed_row_count IS NOT NULL THEN
        UPDATE processing_stats
        SET row_difference_from_yesterday = NEW.processed_row_count - yesterday_processed_row_count,
            percent_difference_from_yesterday = ((NEW.processed_row_count - yesterday_processed_row_count) / yesterday_processed_row_count) * 100
        WHERE processed_date = NEW.processed_date;
    ELSE
        UPDATE processing_stats
        SET row_difference_from_yesterday = 0,
            percent_difference_from_yesterday = 0
        WHERE processed_date = NEW.processed_date;
    END IF;
END//

DELIMITER ;
