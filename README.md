# RDBMS performance comparison
RDBMS performance comparison.

## RDBMS Used
RDBMS used in tests. All RDBMS are executed as Docker containers with default settings.

| DB        | Version   |
|-----------|-----------|
|MySQL      | 8.3.0     |
|PostgreSQL | 16.2      |
|MSSQL      | 2022-CU11 |
|MSSQL      | 2019-CU20 |

[//]: # (## Queries)

[//]: # (### Q1 - Filter table using int column)

[//]: # (### Q2 - Filter table using fixed length char column)

[//]: # (### Q3 - Filter table using fixed variable char column)

[//]: # (### Q4 - Filter table using fixed large text column)

[//]: # ()
[//]: # (## Performance Measurement)

[//]: # (Each query was executed 5 times after new DB created to warm up the DB cache. Then it was executed 10 times and average execution time is measured. )

[//]: # ()
[//]: # (## Results)

[//]: # (Below, you'll find the average execution time per each query.)

[//]: # ()
[//]: # (| Test | MySQL        | PostgreSQL   | MSSQL2019    | MSSQL2022    |)

[//]: # (|------|--------------|--------------|--------------|--------------|)

[//]: # (| Q1   | 4.544081137s | 520.342625ms | 317.055512ms | 208.835308ms |)

[//]: # (| Q2   | 5.426971168s | 591.513795ms | 636.309093ms | 448.1736ms   |)

[//]: # (| Q3   | 5.220925704s | 604.711712ms | 676.073775ms | 493.385491ms |)

[//]: # (| Q4   | 5.344265841s | 586.072054ms | 2.375222368s | 2.207073264s |)
