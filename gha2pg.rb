#!/usr/bin/env ruby

require 'pry'
require 'date'
require 'open-uri'
require 'zlib'
require 'stringio'
require 'json'
require 'etc'
require 'pg'

$thr_n = Etc.nprocessors
puts "Available #{$thr_n} processors"

# Set $debug = true to see output for all events generated
# Set $json_out to save output JSON file
# Set $db_out = true if You want to put int PSQL DB
$debug = false
$json_out = false
$db_out = true

# DB setup:
# apt-get install postgresql
#
# sudo -i -u postgres
# psql
# create database gha;
# create user gha_admin with password '<<your_password_here>>';
# grant all privileges on database "gha" to gha_admin;

# Defaults are:
# Database host: environment variable PG_HOST or `localhost`
# Database port: PG_PORT or 5432
# Database name: PG_DB or 'gha'
# Database user: PG_USER or 'gha_admin'
# Database password: PG_PASS || 'password'
def connect_db
  PG::Connection.new(
    host: ENV['PG_HOST'] || 'localhost',
    port: (ENV['PG_PORT'] || '5432').to_i,
    dbname: ENV['PG_DB'] || 'gha',
    user: ENV['PG_USER'] || 'gha_admin',
    password: ENV['PG_PASS'] || 'password'
  ).tap do |con|
    con.exec 'set session characteristics as transaction isolation level repeatable read'
  end
rescue PG::Error => e
  puts e.message
  exit(1)
end

# Create prepared statement, bind args, execute and destroy statement
# This is not creating transaction, but `process_table` calls it inside transaction
# it is called without transaction on `gha_events` table but *ONLY* because each JSON in GHA
# is a separate/unique GH event, so can be processed without concurency check at all
def exec_stmt(con, sid, stmt, args)
  con.prepare sid, stmt
  con.exec_prepared(sid, args).tap do
    con.exec('deallocate ' + sid)
  end
end

# Process 2 queries: 
# 1st is select that checks if element exists in array
# 2nd is executed when element is not present, and is inserting it
# In rare cases of unique key contraint violation, operation is restarted from beginning
def process_table(con, sid, stmts, argss, retr=0)
  res = nil
  con.transaction do |con|
    stmts.each_with_index do |stmt, index|
      args = argss[index]
      res = exec_stmt(con, sid, stmt, args)
      return res if index == 0 && res.count > 0
    end
  end
  res
rescue PG::UniqueViolation => e
  con.exec('deallocate ' + sid)
  # puts "UNIQUE violation #{e.message}"
  exit(1) if retr >= 1
  return process_table(con, sid, stmts, argss, retr + 1)
end

# Write single event to PSQL
def write_to_pg(con, ev)
  sid = 'stmt' + Thread.current.object_id.to_s
  # gha_events
  # {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592, "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592, "org:Hash"=>19451}i#
  # {"id"=>10, "type"=>29, "actor"=>278, "repo"=>290, "payload"=>216017, "public"=>4, "created_at"=>20, "org"=>230}
  eid = ev['id'].to_i
  rs = exec_stmt(con, sid, 'select 1 from gha_events where id=$1', [eid])
  return if rs.count > 0
  exec_stmt(
    con,
    sid,
    'insert into gha_events(id, type, actor_id, repo_id, payload_id, public, created_at, org_id) ' +
    'values($1, $2, $3, $4, $5, $6, $7, $8)',
    [
      eid,
      ev['type'],
      ev['actor']['id'],
      ev['repo']['id'],
      ev['payload'].hash,
      ev['public'],
      Time.parse(ev['created_at']),
      ev['org'] ? ev['org']['id'] : nil
    ]
  )

  # gha_actors
  # {"id:Fixnum"=>48592, "login:String"=>48592, "display_login:String"=>48592, "gravatar_id:String"=>48592, "url:String"=>48592, "avatar_url:String"=>48592}
  # {"id"=>8, "login"=>34, "display_login"=>34, "gravatar_id"=>0, "url"=>63, "avatar_url"=>49}
  act = ev['actor']
  aid = act['id'].to_i
  process_table(
    con,
    sid,
    [
      'select 1 from gha_actors where id=$1', 
      'insert into gha_actors(id, login) ' +
      'values($1, $2)'
    ],
    [
      [aid],
      [
        aid,
        act['login']
      ]
    ]
  )

  # gha_repos
  # {"id:Fixnum"=>48592, "name:String"=>48592, "url:String"=>48592}
  # {"id"=>8, "name"=>111, "url"=>140}
  repo = ev['repo']
  rid = repo['id'].to_i
  process_table(
    con,
    sid,
    [
      'select 1 from gha_repos where id=$1',
      'insert into gha_repos(id, name) ' +
      'values($1, $2)'
    ],
    [
      [rid],
      [
        rid,
        repo['name']
      ]
    ]
  )
end

# Are we interested in this org/repo ?
def repo_hit(data, forg, frepo)
  unless data
    puts "Broken repo name"
    return false
  end
  org, repo = *data.split('/')
  return false unless forg == '' || org == forg
  return false unless frepo == '' || repo == frepo
  true
end

# Parse signe GHA JSON event
def threaded_parse(con, json, dt, forg, frepo)
  h = JSON.parse json
  f = 0
  full_name = h['repo']['name']
  if repo_hit(full_name, forg, frepo)
    eid = h['id']
    if $json_out
      prt = JSON.pretty_generate(h)
      ofn = "jsons/#{dt.to_i}_#{eid}.json"
      File.write ofn, prt 
    end
    write_to_pg(con, h) if $db_out
    puts "Processed: '#{dt}' event: #{eid}" if $debug
    f = 1
  end
  return f
end

# This is a work for single thread - 1 hour of GHA data
# Usually such JSON conatin about 20000 - 50000 singe events
def get_gha_json(dt, forg, frepo)
  con = connect_db
  fn = dt.strftime('http://data.githubarchive.org/%Y-%m-%d-%k.json.gz').sub(' ', '')
  puts "Working on: #{fn}"
  n = f = 0
  open(fn, 'rb') do |json_tmp_file|
    puts "Opened: #{fn}"
    jsons = Zlib::GzipReader.new(json_tmp_file).read
    puts "Decompressed: #{fn}"
    jsons = jsons.split("\n")
    puts "Splitted: #{fn}"
    jsons.each do |json|
      n += 1
      f += threaded_parse(con, json, dt, forg, frepo)
    end
  end
  puts "Parsed: #{fn}: #{n} JSONs, found #{f} matching"
rescue OpenURI::HTTPError => e
  puts "No data yet for #{dt}"
ensure
  con.close if con
end

# Main work horse
def gha2pg(args)
  d_from = parsed_time = DateTime.strptime("#{args[0]} #{args[1]}:00:00+00:00", '%Y-%m-%d %H:%M:%S%z').to_time
  d_to = parsed_time = DateTime.strptime("#{args[2]} #{args[3]}:00:00+00:00", '%Y-%m-%d %H:%M:%S%z').to_time
  org = args[4] || ''
  repo = args[5] || ''
  puts "Running: #{d_from} - #{d_to} #{org}/#{repo}"
  dt = d_from
  if $thr_n > 1
    thr_pool = []
    while dt <= d_to
      thr = Thread.new(dt) { |adt| get_gha_json(adt, org, repo) }
      thr_pool << thr
      dt = dt + 3600
      if thr_pool.length == $thr_n
        thr = thr_pool.first
        thr.join
        thr_pool = thr_pool[1..-1]
      end
    end
    puts "Final threads join"
    thr_pool.each { |thr| thr.join }
  else
    while dt <= d_to
      get_gha_json(dt, org, repo)
      dt = dt + 3600
    end
  end
  puts "All done."
end

# Required args
if ARGV.length < 4
  puts "Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH [org [repo]]"
  exit 1
end

gha2pg(ARGV)
