# frozen_string_literal: true

require 'mongo'

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)

collection = client[:BasicCompanyData]

random = Random.new(1)

results = collection.find.projection('CompanyNumber' => 1)

sampling = []

warn "Number of active companies: #{results.count}"
results.each do |result|
  next if random.rand(1..100) < 100

  registered_number = result.dig('CompanyNumber')
  # warn "#{registered_number}: Sampled!"
  sampling.push(registered_number)
end

warn "Number of sampled companies: #{sampling.size}"
