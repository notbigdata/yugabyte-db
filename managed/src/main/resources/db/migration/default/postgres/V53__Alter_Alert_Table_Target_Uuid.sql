-- Copyright (c) Yugabyte, Inc.
ALTER TABLE alert ADD COLUMN target_uuid uuid, ADD COLUMN target_type varchar(50);
