**CDC log to Scylla S3** is a proof-of-concept feature added on the `scylla` branch. It allows you to read the CDC log using S3 API (so for example from browser, awscli, s3fs). The CDC log is saved as `.csv` files. 

Internally, it works on the CDC level. Before, for each mutation on CDC-enabled table there was a new CDC log row inserted. I extended it by also writing chunks to S3 tables. As we want to insert full-sized chunks, serialized CDC log is first appended to a per-shard buffer. When this buffer gets full, we emit a new chunk (full-sized).

### Usage example
1. Compile and run Scylla from `scylla` branch.
2. Create a new CDC-enabled table. (for the time being be very gentle - only use `int`, `text` types as those are well-tested). Please do NOT insert data to this table yet.
3. Run `python-mock-s3` from `python` branch. This will create all neccessary S3 tables.
4. Insert some data to your CDC-enabled table. Please insert enough data to fill a chunk (`4KB` times number of shards) - maybe few hundred rows? At this time only `INSERT`s have been tested.
5. You can now see the CDC log from `cdc-log` bucket: `http://localhost:8000/cdc-log/` as `.csv` files.
6. You can even mount it via `s3fs`: `mkdir cdclog; s3fs cdc-log cdclog -d -f -s -o url=http://localhost:8000 -o nomultipart`

### Known limitations/TO-DO/Future plans
- Code quality.
- Not tested on many data types, multi-node Scylla clusters.
- Because we write an entire chunk and this chunk boundary can be in the middle of `.csv` row, the last row might be cut "in half" (**that will be soon fixed**).
- The CDC log is divided into multiple S3 objects - exactly 1 per shard. That could cause this file to get very big. Maybe there should be a separate file per time window (a new file every 5 minutes). Another problem is that the files contain all CDC log tables in single file (per shard) - there should be separate file per each CDC log table.
- If you insert not much data, it won't be enough to fill a chunk, so no CDC log entries will be visible on S3. It should be flushed periodically (probably linked to the separate file per time window idea).
- In CDC, there's a great care given that CDC log rows are in the same node/shard as base table rows (achieved via CDC partitioner among others). But inserted chunks are partitioned without this care - chunk can end up on another node/shard than base table row. It could be fixed by slightly changing the S3 schema and introducing a new partitioner.
- Only tested on `INSERT` operations.
- The serialization code assumes that `atomic_cell_or_collection` is always `atomic_cell` (so probably a crash if it is a collection).
- `find_schema` called every time... (should do it once on initalization).
- Applying mutations to all `s3` tables every time a new chunk is added - should only apply a mutation to `chunk` table and not touch other tables.
- `md5`, `creation_date` column not filled correctly. 
- Serialization probably could be done more efficiently.
