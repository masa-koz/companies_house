# frozen_string_literal: true

require 'csv'
require 'mongo'
require 'optparse'

def apply_scale_and_sign(text, scale, sign)
  return nil unless /^[\d,]+$/.match(text)

  number = Integer(text.gsub(/,/, ''))
  unless scale.nil?
    if /^(\d)+$/.match(scale)
      number *= (10**Integer($LAST_MATCH_INFO[1]))
    else
      warn "Invalid format: scale='#{scale}''"
    end
  end
  unless sign.nil?
    if /^(?:\-)?$/.match(sign)
      number = -number if sign == '-'
    else
      warn "Invalid format: sign='#{sign}''"
    end
  end
  number
end

def retrieve_companies(collection)
  companies = {}
  query = {}
  # query['CompanyNumber'] = 118_102

  results = collection.find(query).projection('CompanyNumber' => 1, 'CountryOfOrigin' => 1, 'Accounts.AccountCategory' => 1)
  warn "Number of companies: #{results.count}"
  results.each_with_index do |result, i|
    registered_number = if m = /^(\h{1,8})$/.match(result.dig('CompanyNumber').to_s)
                          '0' * (8 - m[1].size) + (m[1]).to_s
                        else
                          '00000000'
    end
    warn "(#{i + 1}/#{results.count}): #{registered_number}" if (i % 1000) == 0

    country_origin = result.dig('CountryOfOrigin')
    account_category = result.dig('Accounts', 'AccountCategory')
    h = { 'country_origin' => country_origin, 'account_category' => account_category }
    companies[registered_number] = { country_origin: country_origin, account_category: account_category }
  end
  warn "#{results.count}/#{results.count}: #{companies.keys[-1]}"
  companies
end

def retrieve_entries(collection, pattern, names, duration, ignore_sign)
  entries = {}

  query = { 'nonFraction.name': pattern }
  # query['registered_number'] = '00118102'

  results = collection.find(query)
  warn "Number of Dividends: #{results.count}"

  results.each do |result|
    account_name = if /^[^\:]+\:(.+)$/.match(result.dig('nonFraction', 'name'))
                     $LAST_MATCH_INFO[1]
                   else
                     result.dig('nonFraction', 'name')
    end

    found = false
    names.each do |name|
      if account_name == name
        found = true
        break
      end
    end
    next unless found

    registered_number = result.dig('registered_number')

    filing_date = result.dig('filing_date')
    context_ref = result.dig('nonFraction', 'contextRef')
    unit_ref = result.dig('nonFraction', 'unitRef')

    # $and: [{ "context.end_date": {$gt:'2017-12-31'} }, { "context.end_date": {$lt:'2019-01-01'} }]
    query = { 'registered_number': registered_number,
              'filing_date': filing_date,
              'context.id': context_ref }
    unless duration.nil?
      query['$and'] = [{ 'context.end_date': { '$gt': duration[:start] } },
                       { 'context.end_date': { '$lt': duration[:end] } }]
    end

    context = collection.find(query).first
    next if context.nil?

    unit = collection.find('registered_number': registered_number,
                           'filing_date': filing_date,
                           'unit.id': unit_ref).first
    next if unit.nil?

    warn "registered_number: #{registered_number}"

    if entries.key?(registered_number)
      warn "Duplicate entry: registered_number: '#{registered_number}', " \
      "nonFraction.name: '#{result.dig('nonFraction', 'name')}', " \
      "nonFraction.contextRef: '#{result.dig('nonFraction', 'contextRef')}', " \
      "nonFraction.text: '#{result.dig('nonFraction', 'text')}'"
      next
    end

    # instant = context.dig('context', 'instant')
    # forever = context.dig('context', 'forever')

    number = apply_scale_and_sign(result.dig('nonFraction', 'text'),
                                  result.dig('nonFraction', 'scale'), result.dig('nonFraction', 'sign'))
    if number && ignore_sign
      number = -number if number < 0
    end
    unit = unit.dig('unit', 'measure')

    start_date = context.dig('context', 'start_date')
    end_date = context.dig('context', 'end_date')

    entries[registered_number] = { number: number, unit: unit,
                                   start_date: start_date, end_date: end_date }
  end
  entries
end

opt = OptionParser.new
params = {}
opt.on('-o [VAL]') { |v| params[:o] = v }
opt.on('-p VAL') { |v| params[:p] = v }
opt.on('-s [VAL]') { |v| params[:s] = v }
opt.on('-e [VAL]') { |v| params[:e] = v }
opt.parse!(ARGV)

csvname = if params.key?(:o)
            params[:o]
          else
            'accounts.csv'
end

duration_start = if params.key?(:s)
                   params[:s]
                 else
                   '2017-12-31'
end
duration_end = if params.key?(:e)
                 params[:s]
               else
                 '2019-01-01'
end
duration = { start: duration_start, end: duration_end }

pattern = Regexp.compile(params[:p])

# DividendsPaid, DividendsPaidOnShares
# TurnoverRevenue
names = ARGV[0..-1]

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)
acct_collection = client[:accounts]
basic_collection = client[:BasicCompanyData]

companies = retrieve_companies(basic_collection)
entries = retrieve_entries(acct_collection, pattern, names, duration, true)

entries.each do |registered_number, entry|
  if companies.key?(registered_number)
    companies[registered_number].merge!(entry)
  else
    companies[registered_number] = entry
  end
end

# companies[registered_number] = { country_origin: country_origin, account_category: account_category,
#  account_ref_month: account_ref_month, account_ref_day: account_ref_day }
open(csvname, 'w') do |output|
  output.puts %w[registered_number country_origin account_category
                 query_acct_name query_duration_start query_duration_end
                 number unit startDate endDate].to_csv
  companies.each do |registered_number, entry|
    output.puts [registered_number, entry[:country_origin], entry[:account_category],
                 params[:p], duration_start, duration_end,
                 entry[:number], entry[:unit], entry[:start_date], entry[:end_date]].to_csv
  end
end
