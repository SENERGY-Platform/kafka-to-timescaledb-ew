# Timestamp parsing

How `ew/converter.py` turns the time value of a Kafka message into a `datetime`, why it guesses the format instead of trusting the one configured for the export, and what that guessing costs per row.

## Scope

Applies to every value that reaches `to_datetime` in `ew/converter.py`, which is every column an export declares as `TIMESTAMP` or `TIMESTAMPTZ`. The measured numbers were taken on Python 3.12.3, single-threaded, one timestamp column per row, 20000 iterations, best of five runs.

**Not this if**: the timestamps arrive and are written, but shifted by whole hours. That is not the format search described here. A naive `datetime` written into a `TIMESTAMPTZ` column is interpreted in the database session's time zone, and this worker never sets one — check the session time zone before suspecting the format list. The one case where parsing does shift a value by hours is the `unix` path described under "Time zones" below.

Breadth: the parsing rules follow from the standard library and from arithmetic. The per-row costs are single measurements on one machine and one Python version — treat them as orders of magnitude, not as constants.

## The problem

`time_format` is a per-export filter argument, produced by whoever creates the export and consumed here. The worker cannot verify it, cannot derive it from the data, and gets no error when it is wrong — the value is simply a `strptime` pattern that may or may not match what the devices actually send. In practice the format is often unknown to the person creating the export, and often absent.

Before the format search, a value that did not match the configured format raised out of `gen_row`, and `_gen_rows_batch` skipped the row with

```
generating row
```

logged at error level with the exception string and the export id. An export configured with no `time_format` at all was worse than wrong: `datetime.datetime.strptime(val, None)` raises `TypeError` for every single row, so the export silently produced nothing.

## What it does now

`to_datetime(val, fmt, export_id)` tries candidates in this order and returns the first that parses:

1. the format that last worked **for this export**, from `_learned_formats`
2. the format configured for the export
3. `FALLBACK_TIME_FORMATS`, most common first — ISO 8601 with and without fractional seconds and with `Z` or a numeric offset, unix epoch, space-separated variants, dotted and slashed dates, RFC 1123 and RFC 850, compact `YYYYMMDDHHMMSS`, and last a `datetime.fromisoformat` catch-all for ISO variants the explicit patterns miss

Only when every candidate fails is the row skipped, now with `TimeParseError`:

```
could not parse time '<value>' with format '<format>' or any known format
```

The first time a fallback is used for an export, one warning is logged with the export id, the configured format, the format actually used and the value. Repeats of the same pair stay quiet, so a hot loop cannot flood the log.

### Only fallbacks are ever learned

`_learned_formats` is written only when the format that worked was **not** the configured one. That is not an optimisation, it is what bounds the pathological case: because the configured format is always tried directly after the learned one, an export whose rows alternate between the configured format and one fallback costs at most two attempts per row instead of a full search.

### The cache key is the export, not the format

The key has to be the unit that actually has one timestamp format. An export does; a format string does not — several exports share one configured string, and every export without a format shares the absent one. Keyed on the configured format, three exports with differing real formats interleaved in one batch evicted each other's entry on every row and each row paid a full search: 30.6 µs per conversion. Keyed on the export id the same mix costs 2.6 µs.

`get_exports_batch` returns messages from all subscribed topics in consumption order, so that interleaving is the normal case and not a corner one.

### Guessed epochs are range-checked

A numeric value is scaled by 1, 10³, 10⁶ or 10⁹ and accepted as an epoch only if the result lands between `UNIX_GUESS_MIN_SECONDS` and `UNIX_GUESS_MAX_SECONDS` (1990 to 2100). Without that window a compact date is indistinguishable from an epoch: `20220301154501` is a valid 14-digit number, and read as milliseconds it is a plausible-looking timestamp in the year 2610. The window rejects it, and the search falls through to `%Y%m%d%H%M%S`. The same holds for the 8-digit `20220301`, which as epoch seconds would silently become a date in 1970.

The window applies only while **guessing**. An export configured with `unix` is trusted, so a legitimate epoch 0 still parses.

### Time zones

Everything returns a naive `datetime` in UTC. Formats carrying an offset are converted and stripped, which matches what the dominant `%Y-%m-%dT%H:%M:%S.%fZ` path already produced — there the `Z` is a literal and the result is UTC wall-clock.

The `unix` path changed with this: it used `datetime.datetime.fromtimestamp(val)`, which returns **local** time. In the container that is the same thing, because the image sets no `TZ` and defaults to UTC, but on a host with a local time zone the same converter used to mix local and UTC values into one column depending on which branch ran. Mixing them was judged worse than the change.

## Cost per row

A failing `strptime` costs about as much as a succeeding one — the format regex is cached either way, so the failure is a regex mismatch and not a compile. That makes the search cost linear and easy to reason about:

> ≈ 3 µs + 4.4 µs per rejected candidate

| Situation | per conversion | per full 5-column row |
|---|---|---|
| configured format correct | 3.2 µs | 4.1 µs |
| learned format hits (configured one wrong) | 1.2 – 3.1 µs | 4.4 µs |
| 10 exports, all formats mixed | 6.0 µs | — |
| search reaches the 7th candidate (`unix`) | 28 µs | 30 µs |
| search reaches the last candidate | 117 µs | 120 µs |
| nothing parses, row skipped | 113 µs | 116 µs |

For reference, the four non-timestamp columns of a row cost 0.42 µs together. In the normal case the search adds about 13 % to row generation, which is not where the worker spends its time — the Kafka fetch, the JSON decode, `execute_values` and the database round trip are all still there. In the worst case row generation becomes the bottleneck at 8400 rows per second instead of 245000, which is why the cache exists.

## Known limitations

- **One export emitting two different formats, neither of them the configured one**, flips the learned entry on every row: 67.7 µs per conversion. No export is expected to do this.
- **Unparsable values stay expensive**, 113 µs each before the row is skipped. A stream with 20 % unparsable values measures 25.6 µs per row on average.
- **Ambiguous dates are resolved, not reported.** `%d/%m/%Y` is tried before `%m/%d/%Y`, so `01/03/2022` is 1 March. Guessing means a wrong guess writes a wrong timestamp where the old behaviour would have skipped the row. That trade was made deliberately: a dropped message is not recoverable, a wrong format is visible in the data.

## Rejected alternatives

- **Keying the cache on the digit shape of the value** (`str.translate` of digits to `0`, so `2022-03-01T15:45:01.123000Z` becomes `0000-00-00T00:00:00.000000Z`) was measured at 0.32 µs per row and solves the same eviction problem one level deeper — it would also absorb an export emitting two formats. Rejected because the export is the unit that actually has one format, the id is already in scope at the call site, and the shape key adds a second concept to explain.
- **Caching negative results** would cut the 113 µs for unparsable values to nothing, but the only cheap key for it is the value shape, and one shape covers both valid and invalid values: `2022-13-45T99:99:99Z` and a correct ISO timestamp are the same shape. A negative entry would start rejecting good values.
- **Clearing the learned format when an export is deleted.** The entry is a few hundred bytes per export ever seen, and the delete path runs in the table manager's thread. Cross-thread mutation of the cache to reclaim that is not worth it; a stale entry costs one failed attempt and is then relearned.
- **Making the fallback list configurable** was not done. A list that differs per deployment cannot be reasoned about from the code, and the failure it would fix — a format nobody anticipated — is better fixed by adding it here.

## Tests

`tests/test_converter.py` covers the search, the epoch window, the time zone normalisation, the per-export cache and the unparsable case. It loads `ew/converter.py` directly by path because the converter needs nothing but the standard library, while `tests/test_export_worker.py` needs the Kafka and database dependencies and a reachable database.
