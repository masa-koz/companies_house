# frozen_string_literal: true

require 'csv'
require 'mongo'

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)
collection = client[:psc]
collection_basic = client[:BasicCompanyData]

def write_into_csv(result)
  data = result['data']
  voting, voting_kind = data['natures_of_control'].collect do |x|
    if /^voting-rights-(\d{2}-to-\d{2,3})-percent(.*)$/.match(x)
      $LAST_MATCH_INFO[1, 2]
    end
  end .compact[0]
  voting_kind = nil if !voting_kind.nil? && voting_kind.empty?

  ownership, ownership_kind = data['natures_of_control'].collect do |x|
    if /^ownership-of-shares-(\d{2}-to-\d{2,3})-percent(.*)$/.match(x)
      $LAST_MATCH_INFO[1]
    end
  end .compact[0]
  ownership_kind = nil if !ownership_kind.nil? && ownership_kind.empty?

  psc_company_number = if data.dig('address', 'country') != 'Japan'
                         data.dig('identification', 'registration_number')
  end

  [result['company_number'], psc_company_number, data['kind'],
   ownership, ownership_kind,
   voting, voting_kind].to_csv
end

def write_basic_into_csv(company_number, tier, result, psc_info)
  individual_ultimate_psc =
    if psc_info.key?('ultimate_psc')
      psc_info['ultimate_psc'].include?('individual-person-with-significant-control')
    else
      false
    end
  t1_psc = psc_info['t1_psc']

  if result.nil?
    [company_number, nil, nil, individual_ultimate_psc, t1_psc, nil, nil].to_csv
  else
    accounts = result['Accounts']
    [company_number, result['CompanyName'], tier,
     individual_ultimate_psc, t1_psc,
     accounts['AccountCategory'], accounts['AccountRefMonth'],
     accounts['AccountRefDay']].to_csv
  end
end

subsidiaries = {}

cvs_file = open('psc.csv', 'w')
cvs_basic_file = open('basic.csv', 'w')

tier = 1
subsidiaries[tier] = {}
warn "Tier#{tier}:"
collection.find('data.address.country': 'Japan',
                'data.ceased_on': { '$exists': false }).each do |result|
  cvs_file.puts write_into_csv(result)
  STDOUT.flush
  unless subsidiaries[tier].key?(result['company_number'])
    subsidiaries[tier][result['company_number']] = { 'ultimate_psc' => [] }
  end
  ultimate_psc_info =
    subsidiaries[tier][result['company_number']]['ultimate_psc']
  ultimate_psc_info.push(result.dig('data', 'kind'))
  p ultimate_psc_info
end

warn "Number of new Tier#{tier}: #{subsidiaries[tier].size}"
tier += 1
subsidiaries[tier] = {}

until subsidiaries[tier - 1].empty?
  warn "Try to find Tier#{tier} subsidiaries by Tier#{tier - 1} " \
       "subsidiaries(Number of new: #{subsidiaries[tier - 1].size}):"
  subsidiaries[tier - 1].keys.each_with_index do |company_number, i|
    warn "(#{i + 1}/#{subsidiaries[tier - 1].keys.size}) " \
         'Try to find a company which is controlled by' \
         " #{company_number}."

    collection.find('data.identification.registration_number': company_number,
                    'data.kind':
                      'corporate-entity-person-with-significant-control',
                    'data.ceased_on':
                      { '$exists': false }).each do |result|
      warn 'FOUND'
      cvs_file.puts write_into_csv(result)
      STDOUT.flush

      unless subsidiaries[tier].key?(result['company_number'])
        subsidiaries[tier][result['company_number']] = { 't1_psc' => [] }
      end
      if subsidiaries[tier - 1][company_number].key('t1_psc')
        subsidiaries[tier][result['company_number']]['t1_psc'] =
          subsidiaries[tier - 1][company_number]['t1_psc']
      else
        subsidiaries[tier][result['company_number']]['t1_psc'] =
          company_number
      end
    end
  end

  # Exclude the already listed up subsidiaries
  new_subsidiaries_keys = subsidiaries[tier].keys.dup
  1.step(tier - 1) do |x|
    new_subsidiaries_keys -= subsidiaries[x].keys
  end
  subsidiaries[tier] = subsidiaries[tier].slice(*new_subsidiaries_keys)

  warn "Number of new Tier#{tier}: #{subsidiaries[tier].size}"
  tier += 1
  subsidiaries[tier] = {}
end

1.step(tier - 1) do |x|
  subsidiaries[x].each do |company_number, psc_info|
    # Transform company_number as number when it is saved as that on DB.
    result = if company_number =~ /^\h+$/
               collection_basic.find('CompanyNumber': company_number.to_i).first
             else
               collection_basic.find('CompanyNumber': company_number).first
             end
    warn "Cannot find any basic info for #{company_number}" if result.nil?
    cvs_basic_file.puts write_basic_into_csv(company_number, x, result, psc_info)
  end
end

cvs_file.close
cvs_basic_file.close
