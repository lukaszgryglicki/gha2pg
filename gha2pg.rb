#!/usr/bin/env ruby

require 'pry'
require 'date'
require 'open-uri'
require 'zlib'
require 'stringio'
require 'json'
require 'etc'

ncpus = Etc.nprocessors
puts "Available #{ncpus} processors, consider tweaking $thr_n and $thr_m accordingly"
# If You have a powerful network, then prefer to put all CPU power to $thr_n
# For example $thr_n = 48, $thr_m = 1 - will be fastest with 48 CPUs/cores.
$thr_n = 48  # Number of threads to process separate hours in parallel
$thr_m = 1   # Number of threads to process separate JSON events in parallel

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
    puts "Written: #{ofn}"
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
