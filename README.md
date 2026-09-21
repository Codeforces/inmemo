inmemo
======

In-memory storage to avoid requests to database, support hashmap-based indices, Nocturne class-reloading magic. Built upon Jucuzzi.

### Timestamp lookback

Opt in per table, for example: `-DInmemo.TimestampLookback.com.codeforces.module.contest.model.Problem=true`.
Default is off; the property is read only when the updater is created.
For Date indicators after preload, one poll every 30 seconds looks back two minutes from the cursor.
Recovery is limited to that window and the existing row limit; it is not a guaranteed recovery deadline.
Rows are reapplied, so listeners may run repeatedly; consumer caches are not automatically invalidated.
