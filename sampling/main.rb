# frozen_string_literal: true

require 'csv'
require 'enumerable/statistics'
require 'json'
require 'matrix'
require 'mongo'
require 'optparse'

opt = OptionParser.new
params = {}
opt.on('-a [VAL]') { |v| params[:a] = v }
opt.on('-o [VAL]') { |v| params[:o] = v }
opt.on('-r') { |v| params[:r] = v }
opt.on('-s [VAL]') { |v| params[:s] = v }
opt.parse!(ARGV)

all_jsonname = params[:a]
jsonname = params[:s]
csvname = params[:o]

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)

csv_output = if csvname
               open(csvname, 'w')
             else
               STDOUT
             end

registered_numbers = Hash[client[:BasicCompanyData].find(
  'Accounts.AccountCategory' => 'FULL'
).projection('CompanyNumber' => 1).each_with_index.collect do |result, i|
  registered_number = if m = /^0*([1-9]+\d*)$/.match(result.dig('CompanyNumber').to_s)
                        '0' * (8 - m[1].size) + (m[1]).to_s
                      else
                        result.dig('CompanyNumber').to_s
  end
  [registered_number, i]
end]

warn "Number of active companies: #{registered_numbers.keys.size}"

subsidiaries =
  if params[:r]
    _subsidiaries = client[:subsidiaries].find.projection('registered_number' => 1, 'ultimate_psc.country' => 1)
    Hash[_subsidiaries.collect { |v| [v.dig('registered_number'), v.dig('ultimate_psc', 'country')] }]
  else
    {}
  end

count = 0

mean = registered_numbers.values.mean
csv = [count, mean].flatten(1).to_csv
csv_output.puts csv
csv_output.flush
count += 1

count.step(100_000) do
  warn "#{count}:"
  output = if jsonname
             prefix = "#{count}_" unless params[:r].nil?
             open("#{prefix}#{jsonname}", 'w')
           else
             STDOUT
           end

  sample = registered_numbers.keys.sample(registered_numbers.size / 50)

  # sample_subsidiaries = registered_numbers.slice(*(sample & subsidiaries.keys))
  sample_registered_numbers = registered_numbers.slice(*sample)

  mean = sample_registered_numbers.values.mean
  csv = [count, mean].flatten(1).to_csv
  csv_output.puts csv
  csv_output.flush
  
  warn "Number of sample companies: #{sample.size}"

  output.puts sample.to_json
  output.close

  break if params[:r].nil?
end

csv_output.close

if all_jsonname
  all_output = open(all_jsonname, 'w')
  all_output&.puts registered_numbers.to_json
  all_output.close
end
