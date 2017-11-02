BEGIN TRANSACTION;
DROP TABLE IF EXISTS p2p_message;
CREATE TABLE p2p_message (
    "id" INTEGER PRIMARY KEY,
    "message_id" TEXT,
    "response_id" TEXT,
    "message_name" TEXT,
    "message" TEXT,
    "queue" TEXT,
    "is_ingoing" INTEGER,
    "out_sender" TEXT,
    "out_is_delivered" INTEGER,
    "out_delivery_attempts" INTEGER,
    "out_last_attempt_time" TEXT,
    "in_is_handled" INTEGER,
    "in_consumer_id" TEXT,
    "format" TEXT DEFAULT "xml"
);

DROP TABLE IF EXISTS storage;
CREATE TABLE storage (
	"volume_id" TEXT,
	"type" TEXT,
	"device" TEXT,
	"state" TEXT
);

DROP TABLE IF EXISTS state;
CREATE TABLE state (
	"name" TEXT PRIMARY KEY ON CONFLICT REPLACE,
	"value" TEXT
);

DROP TABLE IF EXISTS tasks;
CREATE TABLE tasks (
    "task_id" TEXT PRIMARY KEY,
    "name" TEXT,
    "args" TEXT,
    "kwds" TEXT,
    "state" TEXT,
    "result" TEXT,
    "traceback" TEXT,
    "start_date" TEXT,
    "end_date" TEXT,
    "worker_id" TEXT,
    "soft_timeout" FLOAT,
    "hard_timeout" FLOAT
);

COMMIT;
