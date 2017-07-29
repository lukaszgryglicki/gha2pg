#!/usr/bin/env ruby

require 'pg'
require 'pry'
require './mgetc'

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

def structure
  c = PG::Connection.new(
    host: ENV['PG_HOST'] || 'localhost',
    port: (ENV['PG_PORT'] || '5432').to_i,
    dbname: ENV['PG_DB'] || 'gha',
    user: ENV['PG_USER'] || 'gha_admin',
    password: ENV['PG_PASS'] || 'password'
  )
  puts 'Connected'
  # gha_events
  # {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592, "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592, "org:Hash"=>19451}i#
  c.exec('drop table if exists gha_events')
  c.exec(
    'create table gha_events(' +
    'id bigint not null primary key, ' +
    'type varchar(40) not null, ' +
    'actor_id bigint not null, ' +
    'repo_id bigint not null, ' +
    'payload_id bigint not null, ' +
    'public boolean not null, ' +
    'created_at timestamp not null, ' +
    'org_id bigint default null' +
    ')'
  )
  # gha_actors
  # {"id:Fixnum"=>48592, "login:String"=>48592, "display_login:String"=>48592, "gravatar_id:String"=>48592, "url:String"=>48592, "avatar_url:String"=>48592}
  c.exec('drop table if exists gha_actors')
  c.exec(
    'create table gha_actors(' +
    'id bigint not null primary key, ' +
    'login varchar(100) not null, ' +
    'display_login varchar(100) not null' +
    ')'
  )
rescue PG::Error => e
  puts e.message
  binding.pry
ensure
  c.close if c
  puts 'Done'
end

puts 'This program will recreate DB structure (dropping all existing data)'
print 'Continue? (y/n) '
c = mgetc
puts "\n"
structure if c == 'y'

