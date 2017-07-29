#!/usr/bin/env ruby

require 'json'
require 'pry'

def analysis(jsons)
  n = 0
  occ = {}
  ml = {}
  jsons.each do |json|
    h = JSON.parse(File.read(json)).to_h
    # Leave h "as is" to investigate top level DB table
    # h = h['actor'] # Investigate gha_actors table
    # h = h['repo'] # Investigate gha_repos table
    keys = h.keys
    classes = h.values.map(&:class).map(&:name)
    h.each do |k, v|
      vl = v.to_s.length
      ml[k] = vl if !ml.key?(k) || ml[k] < vl
    end
    keys.zip(classes).each do |k, c|
      kc = k + ':' + c
      occ[kc] = 0 unless occ.key?(kc)
      occ[kc] += 1
    end
    n += 1
  end
  p occ
  p ml
end

analysis(ARGV)

# Top_level: {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592, "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592, "org:Hash"=>19451}
