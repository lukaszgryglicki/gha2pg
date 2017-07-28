# GitHub Archives to PostgreSQL

This tools filters GitHub archive for given date period and given organization, repository
And saves results into JSON files.

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

GitHub archives keeps data as Gzipped JSONs for each hour (24 gzipped JSONs per day).
Single JSON is not a real JSON file, but "\n" newline separated list of JSONs for each GitHub event in that hour.
So this is a JSON array in reality.

We download this gzipped JSON, process it on the fly, creating array of JSON events and
then each single event JSON matching org/repo criteria is saved in `jsons` directory as
N_ID.json where:
- N - given GitHub archive''s JSON hour as UNIX timestamp
- ID - GitHub event ID

Once saved, You can review those JSONs manually (they''re pretty printed)

# Future
Next plan is to create PostgreSQL database and save matching JSONs there.

