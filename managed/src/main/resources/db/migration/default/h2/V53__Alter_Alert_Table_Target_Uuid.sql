-- Copyright (c) Yugabyte, Inc.
ALTER TABLE alert ADD COLUMN target_uuid uuid;
ALTER TABLE alert ADD COLUMN target_type varchar(50);
