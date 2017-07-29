#!/usr/bin/env ruby

require 'json'
require 'pry'

def analysis(jsons)
  n = 0
  occ = {}
  jsons.each do |json|
    h = JSON.parse(File.read(json)).to_h
    # Leav h "as is" to investigate top level DB table
    h = h['actor'] # Investigate gha_actors table
    keys = h.keys
    classes = h.values.map(&:class).map(&:name)
    keys.zip(classes).each do |k, c|
      kc = k + ':' + c
      occ[kc] = 0 unless occ.key?(kc)
      occ[kc] += 1
    end
    n += 1
  end
  p occ
end

analysis(ARGV)

# Top_level: {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592, "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592, "org:Hash"=>19451}
