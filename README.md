# spark showcase

Practical Spark use case

## Job Analysis

```scala
spark.sql(s"""
    |select day, platform,
    |    sum(e_cpm) as ecpm,
    |    sum(clicksrevenueineuro) as click_revenue,
    |    sum(displayrevenueineuro) as display_revenue
    |from enginejoins.display_click_flat_sampled
    |where day >= '$start' and day < '$end'
    |group by day, platform
    |order by day, platform
""".stripMargin)
  .coalesce(1)
```

**Settings**

* 10 cores / exe
* 200 exe
* Hence, 2000 core

1. Why does the number of active tasks go down and up for every 2000 tasks ? We can largely improve
performance is the number of tasks is always constant.

2. Why can `orderBy` reduce the partition number ? remove empty partition ? How does `orderBy` work
in DataFrame and RDD
