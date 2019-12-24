# frozen_string_literal: true

require 'csv'
require 'mongo'

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

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)
collection = client[:accounts]

STDOUT.puts %w[registered_number account_name number unit startDate endDate instant forever].to_csv

dividends = {}

collection.find('nonFraction.name': /Dividends/).each do |result|
  registered_number = result.dig('registered_number')
  filing_date = result.dig('filing_data')
  context_ref = result.dig('nonFraction', 'contextRef')
  unit_ref = result.dig('nonFraction', 'unitRef')
  context = collection.find('registered_number': registered_number,
                            'filing_data': filing_date,
                            'context.id': context_ref).first
  if context.nil?
    warn "No context for '#{context_ref}': registered_number: '#{registered_number}', " \
    "nonFraction.name: '#{result.dig('nonFraction', 'name')}', " \
    "nonFraction.text: '#{result.dig('nonFraction', 'text')}'"
    next
  end

  unit = collection.find('registered_number': registered_number,
                         'filing_data': filing_date,
                         'unit.id': unit_ref).first
  if unit.nil?
    warn "No unit for '#{unit_ref}': registered_number: '#{registered_number}', " \
    "nonFraction.name: '#{result.dig('nonFraction', 'name')}', " \
    "nonFraction.text: '#{result.dig('nonFraction', 'text')}'"
    next
  end

  unless dividends.key?(registered_number)
    dividends[registered_number] = { context: {} }
  end

  non_fraction_name = result.dig('nonFraction', 'name')
  if dividends[registered_number][:context][context_ref] == non_fraction_name
    warn "Duplicate entry: registered_number: '#{registered_number}', " \
    "nonFraction.name: '#{result.dig('nonFraction', 'name')}', " \
    "nonFraction.contextRef: '#{result.dig('nonFraction', 'contextRef')}', " \
    "nonFraction.text: '#{result.dig('nonFraction', 'text')}'"
    next
  end
  dividends[registered_number][:context][context_ref] = non_fraction_name

  start_date = context.dig('context', 'start_date')
  end_date = context.dig('context', 'start_date')
  instant = context.dig('context', 'instant')
  forever = context.dig('context', 'forever')

  unit = unit.dig('unit', 'measure')

  if start_date.nil? && end_date.nil? && instant.nil? && forever.nil?
    warn "No startDate/EndDate/instant/forever': registered_number: '#{registered_number}', " \
    "nonFraction.name: '#{result.dig('nonFraction', 'name')}', " \
    "nonFraction.text: '#{result.dig('nonFraction', 'text')}'"
    next
  end

  number = apply_scale_and_sign(result.dig('nonFraction', 'text'),
                                result.dig('nonFraction', 'scale'), result.dig('nonFraction', 'sign'))
  account_name = if /^[^\:]+\:(.+)$/.match(result.dig('nonFraction', 'name'))
                   $LAST_MATCH_INFO[1]
                 else
                   result.dig('nonFraction', 'name')
  end
  STDOUT.puts [registered_number, account_name, number, unit, start_date, end_date, instant, forever].to_csv
end
