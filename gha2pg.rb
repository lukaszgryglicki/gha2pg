#!/usr/bin/env ruby

require 'pry'
require 'date'
require 'open-uri'
require 'zlib'
require 'stringio'
require 'json'

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

def get_gha_json(dt, forg, frepo)
  fn = dt.strftime('http://data.githubarchive.org/%Y-%m-%d-%k.json.gz').sub(' ', '')
  n = f = 0
  open(fn, 'rb') do |json_tmp_file|
    jsons = Zlib::GzipReader.new(json_tmp_file).read
    jsons.split("\n").each do |json|
      h = JSON.parse json
      full_name = h['repo']['name']
      n += 1
      if repo_hit(full_name, forg, frepo)
        eid = h['id']
        prt = JSON.pretty_generate(h)
        ofn = "jsons/#{dt.to_i}_#{eid}.json"
        File.write ofn, prt
        puts "Written: #{ofn}"
        f += 1
      end
    end
  end
  puts "#{fn}: parsed #{n} JSONs, found #{f} matching"
rescue OpenURI::HTTPError => e
  puts "No data yet for #{dt}"
end

def gha2pg(args)
  d_from = parsed_time = DateTime.strptime("#{args[0]} #{args[1]}:00:00+00:00", '%Y-%m-%d %H:%M:%S%z').to_time
  d_to = parsed_time = DateTime.strptime("#{args[2]} #{args[3]}:00:00+00:00", '%Y-%m-%d %H:%M:%S%z').to_time
  org = args[4] || ''
  repo = args[5] || ''
  # puts "#{d_from} - #{d_to} #{org}/#{repo}"
  dt = d_from
  while dt <= d_to
    # puts dt
    get_gha_json(dt, org, repo)
    dt = dt + 3600
  end
end

if ARGV.length < 4
  puts "Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH [org [repo]]"
  exit 1
end

gha2pg(ARGV)
