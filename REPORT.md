# Assignment #3: Report

**Name: Arham Hussain Inamdar**

**Student ID: 801404110**

**Email: ainamda3@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801404110

The commands you ran, in order:

```bash
python3 datagen.py 801404110

docker compose -f docker-compose.codespaces.yml up -d

docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py 801404110 \
  /opt/spark/work-dir/shared/input

docker cp main.py spark-master:/opt/spark/work-dir/

set -o pipefail
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output \
  2>&1 | tee run.txt

docker compose down
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

`I used try_cast on fare so an empty value or something like N/A would turn into null instead of stopping the job. Then I counted trips where the dropoff was not after pickup and trips where the fare was null. I filtered out those rows and kept the ones that passed both checks. For the removed count, I counted a trip only once even if it failed both rules.`

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

`I kept fare as a string when I read the file so I could see which values were invalid. If I had read it as a DOUBLE, Spark would have changed bad values to null right away, so I would not have been able to count them separately before cleaning the data.`

The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|   100000   |   1447   |   1511   |  2958   |  97042    |

Two or three bad rows, as they appear in `trips.csv`:

```
T000003,D121,6,4,2026-09-11 23:21:46,2026-09-11 23:08:03,2.9,12.92,cash
T000013,D021,15,5,2026-09-02 09:37:11,2026-09-02 09:17:19,5.55,20.09,cash
```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
day,zone_id,zone_name,trips,revenue
2026-09-01,1,Uptown Center,290,5801.05
2026-09-01,2,First Ward,301,6054.85
2026-09-01,3,Fourth Ward,186,3728.78
2026-09-01,4,South End,252,5001.53
2026-09-01,5,Dilworth,156,3070.38
2026-09-01,6,Plaza Midwood,120,2259.81
2026-09-01,7,NoDa,164,3195.15
2026-09-01,8,University City,233,4450.91
2026-09-01,9,Ballantyne,137,2688.97
2026-09-01,10,SouthPark,136,2548.24
```

`For September 1, First Ward had the most trips and Uptown Center had the most revenue out of the ten zones shown.`

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
driver_id,day,trips
D001,2026-09-15,20
D002,2026-09-06,27
D003,2026-09-04,17
D004,2026-09-28,21
D005,2026-09-04,25
D006,2026-09-19,19
D007,2026-09-18,22
D008,2026-09-08,23
D009,2026-09-27,21
D010,2026-09-11,19
```

`These rows show the busiest day for each of the first ten drivers. D002 had the most trips in this group, with 27 on September 6.`

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
SELECT
    z.borough,
    COUNT(*) AS trips,
    ROUND(SUM(t.fare), 2) AS revenue,
    ROUND(AVG(t.distance_km), 2) AS avg_distance_km
FROM trips AS t
INNER JOIN zones AS z
    ON t.pickup_zone = z.zone_id
GROUP BY z.borough
ORDER BY revenue DESC
```

```
borough,trips,revenue,avg_distance_km
Central,39381,781971.35,5.39
South,16739,331935.65,5.38
North,15466,303391.94,5.31
East,13014,259109.01,5.39
West,12442,247436.79,5.4
```

`Central had the most trips and the highest revenue. The results are sorted from highest to lowest revenue, so West is at the bottom.`

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
 +- Sort [day#25 ASC NULLS FIRST, zone_id#96 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(day#25 ASC NULLS FIRST, zone_id#96 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1474]
       +- Project [day#25, pickup_zone#2 AS zone_id#96, zone_name#10, trips#80L, revenue#81]
          +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
             :- HashAggregate(keys=[day#25, pickup_zone#2], functions=[count(1), sum(fare#13)])
             :  +- Exchange hashpartitioning(day#25, pickup_zone#2, 200), ENSURE_REQUIREMENTS, [plan_id=1467]
             :     +- HashAggregate(keys=[day#25, pickup_zone#2], functions=[partial_count(1), partial_sum(fare#13)])
             :        +- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#13, cast(pickup_ts#4 as date) AS day#25]
             :           +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
             :              +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
             +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1470]
                +- Filter isnotnull(zone_id#9)
                   +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
 +- Sort [revenue#171 DESC NULLS LAST], true, 0
    +- Exchange rangepartitioning(revenue#171 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1524]
       +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#13), avg(distance_km#6)])
          +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1521]
             +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#13), partial_avg(distance_km#6)])
                +- Project [distance_km#6, fare#13, borough#11]
                   +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                      :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#13]
                      :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                      :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1516]
                         +- Filter isnotnull(zone_id#9)
                            +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

`Both plans read from trips.csv and zones.csv. They use the same kind of join, an inner BroadcastHashJoin, and Spark broadcasts zones because it is the smaller table. For part 2a, Spark groups trips by day and pickup zone before joining, then uses an exchange to group those keys and another range exchange to put the results in order. For part 3, it joins first, groups the results by borough, and exchanges data for that aggregation and the final sort by revenue. Even though I wrote one query with the DataFrame API and the other in SQL, Spark's plans have a lot in common: both use the same join and partial and final hash aggregation.`


---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

`I would keep the validation rules and the DataFrame and SQL operations, since Spark can run them across the workers. The first thing I would change is writing each result as one file. That is required for this assignment, but with 200 million trips I would let Spark write multiple files so one task does not have to handle all the output. I would also reuse the cleaned data in Parquet for the analyses instead of making Spark read and clean the CSV again each time. After that, I would check the partitions and cluster resources based on how the run performs.`


---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

`I didn't run into any errors`