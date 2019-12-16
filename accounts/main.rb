# frozen_string_literal: true

require 'zip/zip'
require 'rexml/document'
require 'rexml/xpath'
require 'csv'

class UKAccounts
  def initialize(target_dir)
    @target_dir = target_dir
  end

  def get_accounts_from_context(doc, namespaces, context, type)
    accounts = []
    context.each do |key, val|
      next unless val[:type] == type

      elements_ix = doc.get_elements("//#{namespaces[:ix]}:*")
      element = REXML::XPath.first(elements_ix,
                                   "//#{namespaces[:ix]}:*[@contextRef='#{key}']")
      accounts.push([val[:period], element.text])
    end
    accounts
  end

  def get_accounts_from_elements(doc, namespaces, context, type)
    accounts = []
    elements_ix = doc.get_elements("//#{namespaces[:ix]}:*")
    REXML::XPath.each(elements_ix,
                      "//#{namespaces[:ix]}:*[@name='#{type}']") do |element|
      val = context[element.attributes['contextRef']]
      period = val.nil? ? '' : val[:period]
      accounts.push([period, element.text])
    end
    accounts
  end

  def parse_html(data)
    doc = REXML::Document.new(data)

    namespaces = {}
    doc.root.attributes.each_value do |val|
      next unless /^xmlns\:([^=]+)=(.+)$/.match(val.to_string)

      namespace = $LAST_MATCH_INFO[1]
      url = $LAST_MATCH_INFO[2]
      case url
      when %r{^'http\:\/\/www\.xbrl\.org\/[^/]+\/instance\'$}
        namespaces[:xbrli] = namespace
      when %r{^'http\:\/\/xbrl\.org\/[^/]+\/xbrldi'$}
        namespaces[:xbrldi] = namespace
      when %r{^\'http\://www\.xbrl\.org/[^/]+/inlineXBRL\'$}
        namespaces[:ix] = namespace
      when %r{^'http\:\/\/xbrl\.frc\.org\.uk\/fr\/[^/]+\/core\'$}
        namespaces[:core] = namespace
      when %r{^\'http\://xbrl\.frc\.org\.uk/cd/[^/]+/business\'$}
        namespaces[:bus] = namespace
      end
    end

    p namespaces

    context = {}

    elements_xbrli_context = doc.get_elements("//#{namespaces[:xbrli]}:context")

    elements_xbrli_context.each do |element|
      id = element.attributes['id']
      type = REXML::XPath.first(element, "#{namespaces[:xbrli]}:entity/#{namespaces[:xbrli]}:segment/" \
        "#{namespaces[:xbrldi]}:explicitMember")
      period = REXML::XPath.first(element, "#{namespaces[:xbrli]}:period/#{namespaces[:xbrli]}:instant")
      context[id] = { type: type.nil? ? '' : type.text,
                      period: period.nil? ? '' : period.text }
    end

    elements_ix = doc.get_elements("//#{namespaces[:ix]}:*")
    element = REXML::XPath.first(elements_ix,
                                 "//#{namespaces[:ix]}:nonNumeric" \
                                 "[@name='#{namespaces[:bus]}:UKCompaniesHouseRegisteredNumber']")
    company_number = element.nil? ? '' : element.text

    element = REXML::XPath.first(elements_ix,
                                 "//#{namespaces[:ix]}:nonNumeric" \
                                 "[@name='#{namespaces[:bus]}:EntityCurrentLegalOrRegisteredName']")
    company_name = element.nil? ? '' : element.text

    accounts = get_accounts_from_context(doc, namespaces, context, "#{namespaces[:core]}:RetainedEarningsAccumulatedLosses")
    accounts.each do |account|
      puts [company_number, company_name, "RetainedEarningsAccumulatedLosses", account[0], account[1]].to_csv
    end

    accounts = get_accounts_from_elements(doc, namespaces, context, "#{namespaces[:core]}:FixedAssets")
    accounts.each do |account|
      puts [company_number, company_name, "FixedAssets", account[0], account[1]].to_csv
    end

  end

  def parse_xml(data); end

  def read_file(filename)
    Zip::ZipFile.open(filename) do |zipfile|
      zipfile.each do |entry|
        data = zipfile.read(entry.name)
        open(entry.name, 'w') { |f| f.print data }
        case entry.name
        when /\.html$/
          warn entry.name.to_s
          parse_html(data)
        when /\.xml$/
          parse_xml(data)
        else
          next
        end
      end
    end
  end

  def read_dir
    Dir.chdir(@target_dir) do
      Dir.glob('*.zip') do |filename|
        read_file(filename)
        break
      end
    end
  end
end

exit(1) if ARGV.empty?

dirname = ARGV.shift
accounts = UKAccounts.new(dirname)
accounts.read_dir
