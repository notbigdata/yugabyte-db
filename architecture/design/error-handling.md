# Error handling

We don't use exceptions in YugabyteDB's C++ codebase. Instead, we use our own versatile `Status` class and return instances of that class from functions to indicate errors. Look at [`status.h`](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/util/status.h) for the Status interface.

## Contents of a Status instance

A Status instance has the following parts:
- The main status code. This could be "OK", "Not found", "IO error", etc. This is the oldest part of the status code infrastructure and it has some values that are perhaps too specific for the system-wide framework, but they have to stay there for backward compatibility. These status codes are also duplicated in `wire_protocol.proto` in the `ErrorCode` enum inside of the `AppStatusPB` message definition.
- Use-case-specific error code categories. They have been created to avoid making `status.h` a central bottleneck of the codebase that knows about all specific types of errors. Definitions of these categories can be most easily found by grepping the codebase for the string `static constexpr uint8_t kCategory =`. These include PostgreSQL error codes (also known as SQLSTATE) available as `PgsqlError`, transaction errors (`TransactionError`), tablet server errors (`TabletServerError`), timeout-related errors (`TimeoutError`), and detailed operating system error codes (`Errno`).
