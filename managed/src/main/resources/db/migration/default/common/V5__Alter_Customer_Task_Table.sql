-- Copyright (c) Yugabyte, Inc.

alter table customer_task alter column type TYPE varchar(30);
alter table customer_task alter column target_type TYPE varchar(30);
