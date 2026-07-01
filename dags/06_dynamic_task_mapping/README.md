# Pattern 06: Dynamic Task Mapping

The amount of work is frequently unknown until the run starts. An hourly job might find three files or three hundred. Writing a fixed number of tasks cannot express "one task per thing that showed up". Dynamic task mapping, the Airflow 2 feature `.expand()`, generates one mapped task instance per input at run time, so the DAG shape follows the data.

```
  create_tables --> list_inputs --> process_one [expand over N inputs] --> summarize
                       (returns             (N mapped instances,
                        a list of N)         one per input)
```

- DAG id: `dynamic_task_mapping`
- Target: `core.file_ingest`, one row per input
- Inputs: files under a per-date directory (`include.python_utils.inputs`)

## Why this pattern exists

Before dynamic mapping, people faked variable fan-out in painful ways: a fixed pool of N tasks most of which no-op, or a single task with an internal loop that hides all the parallelism and gives you one log for everything. Both are bad. The fixed pool wastes slots and caps throughput at a guess; the internal loop loses per-item retries, per-item logs, and per-item visibility.

Dynamic mapping fixes this cleanly. An upstream task returns a list, and the mapped task expands into one instance per element. Each element gets its own task instance, its own log, its own retry, and can run in parallel with the others up to your concurrency limits. The count is decided at run time from real data, not at authoring time from a guess.

Here, `list_inputs` discovers the files present for the logical date and returns their paths. `process_one` is mapped over that list, so if four files are present, four `process_one` instances run, and if two are present, two run. The acceptance test drops a known number of inputs and confirms the number of rows loaded matches, which means the number of mapped tasks matched.

## Failure modes (what breaks and when)

- Empty input list. If no inputs are present, zero mapped instances run and `summarize` still runs with an empty list. The DAG succeeds with nothing to do rather than failing. That is usually what you want; if an empty input should be an error, add an explicit check.
- One input fails. Because each input is its own task instance, a single bad file fails only its own mapped instance and can be retried on its own, without re-processing the others.
- Explosive fan-out. A runaway upstream that returns a huge list can create a flood of mapped tasks. Airflow's `max_map_length` and pool and concurrency limits bound this; set them deliberately for untrusted input sizes.
- Non-idempotent mapped writes. A retried mapped instance must not duplicate. Here each instance upserts by `(load_date, input_name)`, so a retry is safe.

## Tradeoffs (why not the naive linear DAG)

A single task looping over inputs is simpler to write and worse to operate: no per-item retries, no per-item logs, no parallelism, and one failure fails the whole batch. Dynamic mapping costs you an upstream task that produces the list and a little care around fan-out limits, and gives you real per-item isolation, parallelism, and observability.

The main tradeoff is that very large maps put pressure on the scheduler and the metadata database (one row per mapped instance). For tens or hundreds of items it is ideal; for millions you would batch the items into chunks and map over the chunks instead.

## Production alternatives (what a large org reaches for)

- Mapping over chunks rather than individual items when the count is very large, so each mapped task handles a batch.
- A data-parallel engine (Spark, Dask, Ray) when the work is a single large computation rather than many independent small ones. Airflow orchestrates the job; the engine does the parallelism.
- Kubernetes-based executors or task pools tuned so that a wide map does not overwhelm the workers.
- Partitioned processing in the warehouse itself (a single set-based SQL statement) when the per-item work is really just SQL.

## Run it

```bash
source scripts/env.sh

# Deliver a variable number of inputs, then run
python -c "from include.python_utils.inputs import drop_input_files; drop_input_files('2024-08-01', 5)"
airflow dags test dynamic_task_mapping 2024-08-01

# Or prove the mapped count matches inputs with the acceptance test
pytest tests/acceptance/test_pattern_06_mapping.py -m acceptance -v
```
