#!/usr/bin/env ruby

require 'pry'
require 'date'
require 'open-uri'
require 'zlib'
require 'stringio'
require 'json'
require 'etc'
require 'pg'

ncpus = Etc.nprocessors
puts "Available #{ncpus} processors, consider tweaking $thr_n and $thr_m accordingly"
# If You have a powerful network, then prefer to put all CPU power to $thr_n
# For example $thr_n = 48, $thr_m = 1 - will be fastest with 48 CPUs/cores.
$thr_n = 48   # 48 Number of threads to process separate hours in parallel
$thr_m = 1   # Number of threads to process separate JSON events in parallel
$debug = false
$con = nil

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
  $con = PG::Connection.new(
    host: ENV['PG_HOST'] || 'localhost',
    port: (ENV['PG_PORT'] || '5432').to_i,
    dbname: ENV['PG_DB'] || 'gha',
    user: ENV['PG_USER'] || 'gha_admin',
    password: ENV['PG_PASS'] || 'password'
  )
  puts "Connected"
rescue PG::Error => e
  puts e.message
  exit(1)
end

# gha_events
# {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592, "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592, "org:Hash"=>19451}i#

def exec_stmt(stmt, args)
  sid = 'stmt' + Thread.current.object_id.to_s
  $con.prepare sid, stmt
  $con.exec_prepared(sid, args).tap do
    $con.exec('deallocate ' + sid)
  end
end

def write_to_pg(ev)
  eid = ev['id'].to_i
  rs = exec_stmt('select 1 from gha_events where id=$1', [eid])
  return if rs.count > 0
  exec_stmt(
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
  act = ev['actor']
  aid = act['id'].to_i
  rs = exec_stmt('select 1 from gha_actors where id=$1', [aid])
  if rs.count == 0
    exec_stmt(
      'insert into gha_actors(id, login, display_login) ' +
      'values($1, $2, $3)',
      [
        aid,
        act['login'],
        act['display_login']
      ]
    )
  end
end

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

def threaded_parse(json, dt, forg, frepo)
  h = JSON.parse json
  f = 0
  full_name = h['repo']['name']
  if repo_hit(full_name, forg, frepo)
    eid = h['id']
    prt = JSON.pretty_generate(h)
    ofn = "jsons/#{dt.to_i}_#{eid}.json"
    File.write ofn, prt
    puts "Written: #{ofn}" if $debug
    write_to_pg(h)
    f = 1
  end
  return f
end

def get_gha_json(dt, forg, frepo)
  fn = dt.strftime('http://data.githubarchive.org/%Y-%m-%d-%k.json.gz').sub(' ', '')
  puts "Working on: #{fn}"
  n = f = 0
  open(fn, 'rb') do |json_tmp_file|
    puts "Opened: #{fn}"
    jsons = Zlib::GzipReader.new(json_tmp_file).read
    puts "Decompressed: #{fn}"
    thr_pool = []
    jsons = jsons.split("\n")
    puts "Splitted: #{fn}"
    if $thr_m > 1
      jsons.each do |json|
        n += 1
	# This was passing copy of local `json` to the Thread - but proved unnecessary.
        # thr = Thread.new(json) { |ajson| threaded_parse(ajson, dt, forg, frepo) }
        thr = Thread.new { threaded_parse(json, dt, forg, frepo) }
        thr_pool << thr
        if thr_pool.length == $thr_m
          thr = thr_pool.first
          thr.join
          f += thr.value
          thr_pool = thr_pool[1..-1]
        end
      end
      thr_pool.each do |thr|
        thr.join
        f += thr.value
      end
    else
      jsons.each do |json|
        n += 1
        f += threaded_parse(json, dt, forg, frepo)
      end
    end
  end
  puts "Parsed: #{fn}: #{n} JSONs, found #{f} matching"
rescue OpenURI::HTTPError => e
  puts "No data yet for #{dt}"
end

def gha2pg(args)
  connect_db
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
    thr_pool.each { |thr| thr.join }
  else
    while dt <= d_to
      get_gha_json(dt, org, repo)
      dt = dt + 3600
    end
  end
  puts "All done."
end

if ARGV.length < 4
  puts "Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH [org [repo]]"
  exit 1
end

gha2pg(ARGV)
