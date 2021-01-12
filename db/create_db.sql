CREATE SCHEMA offers;

CREATE TYPE offers.ExcelOffer AS
(
    offer_id INT,
    offer_name VARCHAR(255),
    price INT,
    quantity INT,
    seller_id INT,
    available bool
);

CREATE TYPE offers.OutputOffer AS
(
    offer_id INT,
    offer_name VARCHAR(255),
    price INT,
    quantity INT,
    seller_id INT,
    seller_name VARCHAR(255)
);

CREATE TYPE offers.TaskInfo AS
(
    task_id INT,
    start_date TIMESTAMP,
    finish_date TIMESTAMP,
    status VARCHAR(30),
    num_errors INT,
    num_created INT,
    num_updated INT,
    num_deleted INT,
    seller_id INT,
    seller_name VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE offers.Seller
(
    seller_id   INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    seller_name VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE offers.Task
(
    task_id     INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    start_date  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finish_date TIMESTAMP NULL,
    status      VARCHAR(30) NOT NULL DEFAULT 'Выполняется',
    seller_id   INT REFERENCES offers.Seller (seller_id),
    num_errors  INT NULL,
    num_created INT NULL,
    num_updated INT NULL,
    num_deleted INT NULL,
    CONSTRAINT CK_Status CHECK ( status IN ('Выполняется', 'Завершен', 'Ошибка') )
);

CREATE TABLE offers.Offer
(
    offer_id   INT,
    offer_name VARCHAR(255) NOT NULL,
    price      INT          NOT NULL,
    quantity   INT          NOT NULL,
    seller_id  INT REFERENCES offers.Seller (seller_id),
    CONSTRAINT PK_Offer PRIMARY KEY (offer_id, seller_id)
);

CREATE
OR REPLACE FUNCTION offers.insert_seller(_seller_name VARCHAR(255)) RETURNS INT AS
    $$
BEGIN
INSERT INTO offers.Seller(seller_name)
VALUES (_seller_name);
RETURN currval('offers.seller_seller_id_seq');
END;
    $$
LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION offers.get_offers(_seller_id INT DEFAULT NULL, _offer_id INT DEFAULT NULL,
                                  _offer_name VARCHAR DEFAULT NULL,
                                  _ignore_register BOOL DEFAULT FALSE) RETURNS SETOF offers.OutputOffer AS
$$
BEGIN
RETURN QUERY EXECUTE '
SELECT offer_id,
       offer_name,
       price,
       quantity,
       S.seller_id,
       seller_name
FROM offers.Seller AS S
    JOIN offers.Offer AS O On S.seller_id = O.seller_id'
                    || CASE WHEN _seller_id IS NOT NULL THEN ' AND S.seller_id = ' || _seller_id ELSE '' END
                    || CASE WHEN _offer_id IS NOT NULL THEN ' AND offer_id = ' || _offer_id ELSE '' END
        || CASE
               WHEN _offer_name IS NOT NULL THEN ' AND offer_name'  || CASE WHEN _ignore_register = TRUE THEN ' ILIKE ' ELSE ' LIKE '
END || quote_literal('%' || _offer_name || '%')
               ELSE ''
|| 'ORDER BY offer_name'
END;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION offers.insert_task(_seller_id INT) RETURNS INT AS
    $$
BEGIN
INSERT INTO offers.Task(seller_id)
VALUES (_seller_id);
RETURN currval('offers.task_task_id_seq');
END;
    $$
LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION offers.load_offers(errors INT, _task_id INT, json_data json) RETURNS VOID AS
$$
BEGIN
WITH from_json AS (
    SELECT T.offer_id, T.seller_id, T.offer_name, T.price, T.quantity, T.available
    FROM json_populate_recordset(NULL::offers.ExcelOffer, json_data) AS T
),
     insert_buffer AS (
INSERT
INTO offers.Offer (offer_id, offer_name, price, quantity, seller_id)
SELECT offer_id,
       offer_name,
       price,
       quantity,
       seller_id
FROM from_json AS T
WHERE available = true
  AND NOT EXISTS(SELECT *
                 FROM offers.Offer AS O
                 WHERE T.seller_id = o.seller_id
                   AND T.offer_id = O.offer_id)
    RETURNING 1 AS inserted
         ),
         error_buffer AS (
SELECT 1 AS errors
FROM from_json AS T
WHERE available = false
  AND NOT EXISTS (SELECT *
    FROM offers.Offer AS O
    WHERE T.seller_id = o.seller_id
  AND T.offer_id = O.offer_id)
    )
    , update_buffer AS (
UPDATE offers.Offer
SET
    offer_name = T.offer_name,
    price = T.price,
    quantity = T.quantity
FROM from_json AS T
WHERE available = true
  AND T.seller_id = offers.Offer.seller_id
  AND T.offer_id = offers.Offer.offer_id
    RETURNING 1 AS updated
    )
    , delete_buffer AS (
DELETE
FROM offers.Offer AS O
WHERE EXISTS (SELECT *
    FROM from_json AS T
    WHERE available = false
  AND T.seller_id = O.seller_id
  AND T.offer_id = O.offer_id)
    RETURNING 1 AS deleted
    )
UPDATE offers.Task
SET num_created = (SELECT COUNT(*) FROM insert_buffer),
    num_errors  = (SELECT COUNT(*) FROM error_buffer) + errors,
    num_updated = (SELECT COUNT(*) FROM update_buffer),
    num_deleted = (SELECT COUNT(*) FROM delete_buffer),
    finish_date = CURRENT_TIMESTAMP,
    status      = 'Завершен'
WHERE offers.Task.task_id = _task_id;
END;
$$
LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION offers.get_all_tasks(task_limit INT DEFAULT NULL, task_offset INT DEFAULT NULL) RETURNS SETOF offers.TaskInfo AS
$$
BEGIN
RETURN QUERY(
    SELECT task_id, start_date,
                    finish_date,
                    status,
                    num_errors,
                    num_created,
                    num_updated,
                    num_deleted,
                    S.seller_id,
                    S.seller_name,
                    S.created_at FROM offers.Seller AS S
                 JOIN offers.Task AS T ON S.seller_id = T.seller_id
        ORDER BY CASE WHEN finish_date IS NULL THEN 0 ELSE 1 END,
                    start_date DESC
        OFFSET task_offset LIMIT task_limit
    );
END;
$$
LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION offers.get_task(_task_id INT) RETURNS SETOF offers.TaskInfo AS
$$
BEGIN
RETURN QUERY(
    SELECT task_id, start_date,
                    finish_date,
                    status,
                    num_errors,
                    num_created,
                    num_updated,
                    num_deleted,
                    S.seller_id,
                    S.seller_name,
                    S.created_at FROM offers.Seller AS S
                 JOIN offers.Task AS T ON S.seller_id = T.seller_id
        WHERE task_id = _task_id
    );
END;
$$
LANGUAGE plpgsql;