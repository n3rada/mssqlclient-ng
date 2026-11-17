# Platform constraints

Quick reference for what works where. Use `!info` and `!rds-detect` to fingerprint the target.

## Feature matrix

| Capability | On-premises | Azure SQL DB | Azure MI | Amazon RDS |
|---|:---:|:---:|:---:|:---:|
| xp_cmdshell | Yes | No | Yes | No |
| OLE Automation | Yes | No | Yes | No |
| CLR assemblies | Yes | No | Yes | No |
| Linked servers | Yes | Limited | Yes | Yes |
| sp_configure | Yes | No | Yes | No |
| BACKUP / RESTORE | Native | No | Yes | Only via `rds_*` to S3 |
| Agent jobs | Yes | No | Yes | Yes |
| OPENROWSET (bulk) | Yes | No | Yes | Yes |
| TRUSTWORTHY escalation | Yes | No | Yes | No |
| Password hashes | Yes | No | Yes | Own only* |
| Service account visible | Yes | No | Yes | No |

*\* RDS master user lacks sysadmin — only the current login's hash is visible via `sys.sql_logins`.*

## Amazon RDS specifics

> Reference: [Amazon RDS for SQL Server documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SQLServer.html), [RDS stored procedures](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.SQLServer.CommonDBATasks.html), [S3 integration](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/User.SQLServer.Options.S3-integration.html)

- **Master user** gets `processadmin` + `setupadmin`, not `sysadmin`.
- **S3 integration** via `rds_backup_database`, `rds_restore_database`, `rds_download_from_s3`, `rds_upload_to_s3` — all gated by the instance IAM role.
- **Staging directory** is `D:\S3\` (hardcoded by AWS, max 100 files).
- **IMDS blocked** — 169.254.0.0/16 is denied at the network level.
- **Task history** via `rds_fn_task_status` reveals S3 bucket ARNs and KMS keys (36-day retention).
- **Error log** readable via `rdsadmin.dbo.rds_read_error_log`.

## Azure SQL Database specifics

- **No server-level access** — database-scoped only.
- **No OS access** — no xp_cmdshell, no file system, no OLE.
- **Engine edition 5** (`SERVERPROPERTY('EngineEdition')`).
- **Contained database users** — no server logins.

## Azure Managed Instance specifics

- Closest to on-premises. Most features work.
- **Engine edition 8**.
- Cross-database queries and linked servers work.
- CLR, Agent jobs, and TRUSTWORTHY are available.

## Detection

```
!info              -- @@VERSION reveals Azure SQL / Managed Instance
!rds-detect        -- checks for rdsadmin database and rds_* procedures
```
