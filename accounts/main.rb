# frozen_string_literal: true

require 'zip/zip'
require 'rexml/document'
require 'rexml/xpath'
require 'csv'

class UKAccounts
  def initialize(target_dir)
    @target_dir = target_dir
  end

  def self.load_namespaces(doc)
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
    namespaces
  end

  def self.load_unit(doc, namespaces)
    unit = {}

    # xbrli_units = doc.get_elements()

    doc.root.each_element("//#{namespaces[:xbrli]}:unit") do |xbrli_unit|
      id = xbrli_unit.attributes['id']
      measure = REXML::XPath.first(xbrli_unit, "#{namespaces[:xbrli]}:measure")
      next if measure.nil?

      case measure.text
      when /^iso4217\:(.+)$/
        unit[id] = $LAST_MATCH_INFO[1]
      when "#{namespaces[:xbrli]}:pure"
        unit[id] = ''
      end
    end
    unit
  end

  def self.load_context(doc, namespaces)
    context = {}

    elements = doc.get_elements("//#{namespaces[:xbrli]}:context")

    elements.each do |xbrli_context|
      id = xbrli_context.attributes['id']
      context[id] = {}
      explicit_member = REXML::XPath.first(xbrli_context, "#{namespaces[:xbrli]}:entity/#{namespaces[:xbrli]}:segment/" \
        "#{namespaces[:xbrldi]}:explicitMember")
      unless explicit_member.nil?
        context[id][:name] = explicit_member.text
        context[id][:dimension] = explicit_member.attributes['dimension']
      end

      period = REXML::XPath.first(xbrli_context, "#{namespaces[:xbrli]}:period")
      period = UKAccounts.parse_period(period, namespaces[:xbrli])
      context[id][:period] = period
    end
    context
  end

  def self.parse_period(element, namespace)
    period = {}
    start_date = REXML::XPath.first(element, "#{namespace}:startDate")
    unless start_date.nil?
      end_date = REXML::XPath.first(element, "#{namespace}:endDate")
      if end_date.nil?
        warn 'Invalid format: missing an element of endDate'
      else
        period[:startDate] = start_date.text
        period[:endDate] = end_date.text
      end
      return period
    end

    instant = REXML::XPath.first(element, "#{namespace}:instant")
    unless instant.nil?
      period[:instant] = instant.text
      return period
    end

    forever = REXML::XPath.first(element, "#{namespace}:forever")
    unless instant.nil?
      period[:forever] = true
      return period
    end

    period
  end

  def self.apply_scale_and_sign(number, scale, sign)
    return nil unless /^[\d,]+$/.match(number)

    number = Integer(number.gsub(/,/, ''))
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

  def self.get_accounts_from_context(doc, namespaces, unit, account_name)
    accounts = []
    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    REXML::XPath.each(doc, "//#{namespaces[:xbrldi]}:explicitMember[contains(text(), '#{account_name}')]/" \
      "parent::#{namespaces[:xbrli]}:segment/parent::#{namespaces[:xbrli]}:entity/parent::#{namespaces[:xbrli]}:context") do |context|
      context_ref = context.attributes['id']
      account = REXML::XPath.first(doc, "//#{namespaces[:ix]}:nonFraction[@contextRef='#{context_ref}']")
      next if account.nil?

      unit_ref = account.attributes['unitRef']
      scale = account.attributes['scale']
      sign = account.attributes['sign']
      number = UKAccounts.apply_scale_and_sign(account.text, scale, sign)

      period = REXML::XPath.first(context, "#{namespaces[:xbrli]}:period")
      period = UKAccounts.parse_period(period, namespaces[:xbrli])

      accounts.push([number, unit[unit_ref], period])
    end
    accounts
  end

  def self.get_name_individual_segment(doc, namespaces, context_ref)
    # Try to find an element of NameIndividualSegment which refers to context_ref.
    name_individual_segment =
      REXML::XPath.first(doc, "//#{namespaces[:ix]}:nonNumeric" \
        "[@name='#{namespaces[:core]}:NameIndividualSegment'" \
        " and @contextRef='#{context_ref}']")
    return name_individual_segment.text unless name_individual_segment.nil?

    # Retrieve an element of explicitMember for finding the pair.
    context = REXML::XPath.first(doc.root, "//#{namespaces[:xbrli]}:context[@id='#{context_ref}']")
    return nil if context.nil?

    explicit_member = REXML::XPath.first(context, "#{namespaces[:xbrli]}:entity/#{namespaces[:xbrli]}:segment/" \
      "#{namespaces[:xbrldi]}:explicitMember")
    return nil if explicit_member.nil?

    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    REXML::XPath.each(doc, "//#{namespaces[:xbrldi]}:explicitMember[contains(text(), '#{explicit_member.text}')]/" \
      "parent::#{namespaces[:xbrli]}:segment/parent::#{namespaces[:xbrli]}:entity/parent::#{namespaces[:xbrli]}:context" \
      "[not(@id='context_ref')]") do |element|
      context_ref1 = element.attributes['id']
      name_individual_segment =
        REXML::XPath.first(doc.root, "//#{namespaces[:ix]}:nonNumeric" \
          "[@name='#{namespaces[:core]}:NameIndividualSegment'" \
          " and @contextRef='#{context_ref1}']")
      return name_individual_segment.text unless name_individual_segment.nil?
    end
    nil
  end

  def self.get_accounts_from_elements(doc, namespaces, context, unit, name)
    accounts = []
    doc.root.each_element("//#{namespaces[:ix]}:nonFraction[@name='#{name}']") do |element|
      context_ref = element.attributes['contextRef']
      val = context[context_ref]
      if val.nil?
        warn 'Invalid formant: no contextRef attr'
        return accounts
      end

      name_individual_segment = UKAccounts.get_name_individual_segment(doc, namespaces, context_ref)
      unit_ref = element.attributes['unitRef']
      scale = element.attributes['scale']
      sign = element.attributes['sign']
      number = UKAccounts.apply_scale_and_sign(element.text, scale, sign)

      accounts.push([number, unit[unit_ref], val[:period], name_individual_segment])
    end
    accounts
  end

  def parse_html(data)
    doc = REXML::Document.new(data)

    namespaces = UKAccounts.load_namespaces(doc)
    unit = UKAccounts.load_unit(doc, namespaces)
    context = UKAccounts.load_context(doc, namespaces)

    company_number =
      REXML::XPath.first(doc.root,
                         "//#{namespaces[:ix]}:nonNumeric" \
                         "[@name='#{namespaces[:bus]}:UKCompaniesHouseRegisteredNumber']")
    company_number =
      unless company_number.nil?
        if company_number.has_text?
          company_number.text
        else
          # text may be located under ix's child elements
          company_number.elements.collect(&:text).compact[0]
        end
      end

    account_names = ["#{namespaces[:core]}:TurnoverRevenue",
                     "#{namespaces[:core]}:WagesSalaries",
                     "#{namespaces[:core]}:SocialSecurityCosts",
                     "#{namespaces[:core]}:PensionOtherPost-employmentBenefitCostsOtherPensionCosts",
                     "#{namespaces[:core]}:RetainedEarningsAccumulatedLosses",
                     "#{namespaces[:core]}:FixedAssets",
                     "#{namespaces[:core]}:DividendsPaid"]

    account_names.each do |account_name|
      _namespace, account_name2 = *account_name.split(/\:/)
      accounts = UKAccounts.get_accounts_from_context(doc, namespaces, unit, account_name)
      accounts.each do |account|
        puts [company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end

      accounts = UKAccounts.get_accounts_from_elements(doc, namespaces, context, unit, account_name)
      accounts.each do |account|
        puts [company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end
    end
  end

  def parse_xml(data); end

  def read_file(zipname, file_pattern)
    Zip::ZipFile.open(zipname) do |zipfile|
      zipfile.each do |entry|
        unless file_pattern.nil?
          next unless file_pattern.match(entry.name)
        end
        data = zipfile.read(entry.name)
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

  def read_dir(file_pattern, zip_pattern)
    Dir.chdir(@target_dir) do
      Dir.glob('*.zip') do |zipname|
        unless zip_pattern.nil?
          next unless zip_pattern.match(zipname)
        end
        read_file(zipname, file_pattern)
        break
      end
    end
  end
end

exit(1) if ARGV.empty?

dirname = ARGV.shift
file_pattern = ARGV.shift
zip_pattern = ARGV.shift
file_pattern = Regexp.compile(file_pattern) unless file_pattern.nil?
zip_pattern = Regexp.compile(zip_pattern) unless zip_pattern.nil?

accounts = UKAccounts.new(dirname)
accounts.read_dir(file_pattern, zip_pattern)
