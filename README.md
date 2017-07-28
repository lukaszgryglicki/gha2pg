# GitHub Archives to PostgreSQL

This tools filters GitHub archive for given date period and given organization, repository and saves results into JSON files.

Usage:
./gha2pg.rb YYYY-MM-DD HH YYYY-MM-DD HH [org [repo]]

First two parameters are date from:
- YYYY-MM-DD
- HH

Next two parameters are date to:
- YYYY-MM-DD
- HH

Both next two parameters are optional.
- org (if given and non empty '' then only return JSONs matching given org)
- repo (if given and non empty '' then only return JSONs matching given repo)

You can filter only by org by passing for example 'kubernetes' for org and '' for repo or skipping repo.
You can filter only by repo, You need to pass '' as org and then repo name.
You can return all JSONs byb skipping both params.
You can provide both to observe only events from given org/repo.

Example script that queries for all events from org=`kubernetes` for 5 days:
`./gha2pg.sh`

GitHub archives keeps data as Gzipped JSONs for each hour (24 gzipped JSONs per day).
Single JSON is not a real JSON file, but "\n" newline separated list of JSONs for each GitHub event in that hour.
So this is a JSON array in reality.

We download this gzipped JSON, process it on the fly, creating array of JSON events and
then each single event JSON matching org/repo criteria is saved in `jsons` directory as
N_ID.json where:
- N - given GitHub archive''s JSON hour as UNIX timestamp
- ID - GitHub event ID

Once saved, You can review those JSONs manually (they''re pretty printed)

# Mutithreading

For example cncftest.io server has 48 CPU cores.

Set those Ruby variables:
```
$thr_n = 4  # Number of threads to process separate hours in parallel
$thr_m = 4  # Number of threads to process separate JSON events in parallel
```
If You have a powerful network, then prefer to put all CPU power to `$thr_n`.

For example `$thr_n = 48`, `$thr_m = 1` - will be fastest with 48 CPUs/cores.

# Results
Usually there are about 40000 GitHub events in single hour.
Running this program on a 5 days of data with org `kubernetes` (and no repo set - which means all kubernetes repos).

- Takes: 10 minutes 50 seconds.
- Generates 12002 JSONs in `jsons/` directory with summary size 165 Mb (each JSON is a single GitHub event).
- To do so it processes about 21 Gb of data.
- XZipped file: `kubernetes_events.tar.xz`.

Running this program 1 month of data with org `kubernetes` (and no repo set - which means all kubernetes repos).
June 2017:

- Takes: 61 minutes 26 seconds.
- Generates 60773 JSONs in `jsons/` directory with summary size 815 Mb (each JSON is a single GitHub event).
- To do so it processes about 126 Gb of data.
- XZipped file: `k8s_month.tar.xz`.


# Future
Next plan is to create PostgreSQL database and save matching JSONs there.

