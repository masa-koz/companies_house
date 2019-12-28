# frozen_string_literal: true

require 'json'
require 'mongo'
require 'optparse'

opt = OptionParser.new
params = {}
opt.on('-a [VAL]') { |v| params[:a] = v }
opt.on('-j [VAL]') { |v| params[:j] = v }
opt.parse!(ARGV)

all_jsonname = params[:a]
jsonname = params[:j]

all_output = (open(all_jsonname, 'w') if all_jsonname)

output = if jsonname
           open(jsonname, 'w')
         else
           STDOUT
         end
client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)

collection = client[:BasicCompanyData]

random = Random.new(2)

results = collection.find('Accounts.AccountCategory' => 'FULL').projection('CompanyNumber' => 1)

all = []
sampling = []

warn "Number of active companies: #{results.count}"
results.each do |result|
  registered_number = if m = /^0*([1-9]+\d*)$/.match(result.dig('CompanyNumber').to_s)
                        '0' * (8 - m[1].size) + (m[1]).to_s
                      else
                        result.dig('CompanyNumber').to_s
                      end
  all.push(registered_number)
  next if random.rand(1..100) < 99

  sampling.push(registered_number)
end

warn "Number of sampled companies: #{sampling.size}"

all_output&.puts all.to_json
output.puts sampling.to_json
