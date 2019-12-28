# frozen_string_literal: true

require 'json'
require 'mongo'
require 'optparse'

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

def get_subsidiaries(collection, psc_country)
  subsidiaries = {}

  tier = 1
  subsidiaries[tier] = {}
  warn "Tier#{tier}:"
  collection.find('data.address.country': psc_country,
                  'data.ceased_on': { '$exists': false }).each do |result|
    unless subsidiaries[tier].key?(result['company_number'])
      subsidiaries[tier][result['company_number']] = { ultimate_psc_country: psc_country, has_individual_ultimate_psc: false }
    end
    if result.dig('data', 'kind') == 'individual-person-with-significant-control'
      subsidiaries[tier][result['company_number']][:has_individual_ultimate_psc] = true
    end
  end

  warn "Number of new Tier#{tier}: #{subsidiaries[tier].size}"
  tier += 1
  subsidiaries[tier] = {}

  until subsidiaries[tier - 1].empty?
    # warn "Try to find Tier#{tier} subsidiaries by Tier#{tier - 1} " \
    #     "subsidiaries(Number of new: #{subsidiaries[tier - 1].size}):"
    subsidiaries[tier - 1].keys.each_with_index do |company_number, _i|
      # warn "(#{i + 1}/#{subsidiaries[tier - 1].keys.size}) " \
      #     'Try to find a company which is controlled by' \
      #     " #{company_number}."

      collection.find('data.identification.registration_number': company_number,
                      'data.kind':
                        'corporate-entity-person-with-significant-control',
                      'data.ceased_on':
                        { '$exists': false }).each do |result|
        # warn 'FOUND'

        unless subsidiaries[tier].key?(result['company_number'])
          subsidiaries[tier][result['company_number']] = { ultimate_psc_country: psc_country }
        end

        if subsidiaries[tier - 1][company_number].key(:t1_psc)
          subsidiaries[tier][result['company_number']][:t1_psc] =
            subsidiaries[tier - 1][company_number][:t1_psc]
        else
          subsidiaries[tier][result['company_number']][:t1_psc] =
            company_number
        end
        subsidiaries[tier][result['company_number']][:has_individual_ultimate_psc] =
          subsidiaries[tier - 1][company_number][:has_individual_ultimate_psc]
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
  subsidiaries
end

opt = OptionParser.new
params = {}
opt.on('-C [VAL]') { |v| params[:C] = v }
opt.on('-j [VAL]') { |v| params[:j] = v }
opt.parse!(ARGV)

psc_country = if params.key?(:C)
                params[:C]
              else
                'Japan'
end

jsonname = if params.key?(:j)
             params[:j]
           else
             'japanese_subsidiaries.json'
end

client = Mongo::Client.new(['127.0.0.1:27017'],
                           database: 'uk_companies', monitoring: false)
subsidiaries = get_subsidiaries(client[:psc], psc_country)

output = open(jsonname, 'w')
subsidiaries.each do |tier, sub_subsidiaries|
  sub_subsidiaries.each do |company_number, info|
    json = { registered_number: company_number, tier: tier, t1_psc: info[:t1_psc],
      ultimate_psc: {country: info[:ultimate_psc_country], has_individual: info[:has_individual_ultimate_psc] }}
    output.puts json.to_json
  end
end

output.close
