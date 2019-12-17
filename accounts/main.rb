# frozen_string_literal: true

require 'zip/zip'
require 'rexml/document'
require 'rexml/xpath'
require 'csv'

class UKAccountsDocument
  def initialize(data, filename)
    @doc = REXML::Document.new(data)
    @filename = filename
    @namespaces = {}
    @units = {}
    @contexts = {}
    @parsed = false
  end

  def load_namespaces
    @doc.root.attributes.each_value do |attribute|
      next unless /^xmlns\:([^=]+)=(.+)$/.match(attribute.to_string)

      namespace = $LAST_MATCH_INFO[1]
      url = $LAST_MATCH_INFO[2]
      case url
      when %r{^'http\:\/\/www\.xbrl\.org\/[^/]+\/instance\'$}
        @namespaces[:xbrli] = namespace
      when %r{^'http\:\/\/xbrl\.org\/[^/]+\/xbrldi'$}
        @namespaces[:xbrldi] = namespace
      when %r{^\'http\://www\.xbrl\.org/[^/]+/inlineXBRL\'$}
        @namespaces[:ix] = namespace
      when %r{^'http\:\/\/xbrl\.frc\.org\.uk\/fr\/[^/]+\/core\'$}
        @namespaces[:core] = namespace
      when %r{^\'http\://xbrl\.frc\.org\.uk/cd/[^/]+/business\'$}
        @namespaces[:bus] = namespace
      when %r{^\'http\://www\.companieshouse\.gov\.uk\/ef\/xbrl\/uk\/fr\/gaap\/ae\/[^/]+$}
        @namespaces[:ae] = namespace
      when %r{^\'http\://www\.xbrl\.org\/uk\/fr\/gaap\/pt\/[^/]+$}
        @namespaces[:pt] = namespace
      end
    end
  end

  def load_units
    prefix =
      if @namespaces.key?(:xbrli)
        "#{@namespaces[:xbrli]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}unit") do |unit|
      id = unit.attributes['id']
      measure = REXML::XPath.first(unit, "#{prefix}measure")
      next if measure.nil?

      case measure.text
      when /^iso4217\:(.+)$/
        @units[id] = $LAST_MATCH_INFO[1]
      when "#{prefix}pure"
        @units[id] = ''
      end
    end
  end

  def load_contexts
    prefix =
      if @namespaces.key?(:xbrli)
        "#{@namespaces[:xbrli]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}context") do |context|
      id = context.attributes['id']
      @contexts[id] = {}
      explicit_member = REXML::XPath.first(context, "#{prefix}entity/#{prefix}segment/" \
        "#{prefix}explicitMember")
      unless explicit_member.nil?
        @contexts[id][:name] = explicit_member.text
        @contexts[id][:dimension] = explicit_member.attributes['dimension']
      end

      period = REXML::XPath.first(context, "#{prefix}period")
      period = parse_period(period)
      @contexts[id][:period] = period
    end
  end

  def parse_period(element)
    prefix =
      if @namespaces.key?(:xbrli)
        "#{@namespaces[:xbrli]}:"
      else
        ''
      end

    period = {}
    start_date = REXML::XPath.first(element, "#{prefix}startDate")
    unless start_date.nil?
      end_date = REXML::XPath.first(element, "#{prefix}endDate")
      if end_date.nil?
        warn 'Invalid format: missing an element of endDate'
      else
        period[:startDate] = start_date.text
        period[:endDate] = end_date.text
      end
      return period
    end

    instant = REXML::XPath.first(element, "#{prefix}instant")
    unless instant.nil?
      period[:instant] = instant.text
      return period
    end

    forever = REXML::XPath.first(element, "#{prefix}forever")
    unless instant.nil?
      period[:forever] = true
      return period
    end

    period
  end

  def apply_scale_and_sign(number, scale, sign)
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

  def get_accounts_from_contexts(account_name, account_numeric)
    accounts = []
    return accounts unless @namespaces.key?(:xbrldi)

    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    REXML::XPath.each(@doc, "//#{@namespaces[:xbrldi]}:explicitMember[contains(text(), '#{account_name}')]/" \
      "parent::#{@namespaces[:xbrli]}:segment/parent::#{@namespaces[:xbrli]}:entity/parent::#{@namespaces[:xbrli]}:context") do |context|
      context_ref = context.attributes['id']
      account = REXML::XPath.first(@doc, "//#{@namespaces[:ix]}:nonFraction[@contextRef='#{context_ref}']")
      next if account.nil?

      data = if account_numeric
               unit_ref = account.attributes['unitRef']
               scale = account.attributes['scale']
               sign = account.attributes['sign']
               number = apply_scale_and_sign(account.text, scale, sign)
             else
               account.text
             end

      period = @contexts[context_ref][:period]

      accounts.push([data, @units[unit_ref], period])
    end
    accounts
  end

  def get_name_individual_segment(context_ref)
    # Try to find an element of NameIndividualSegment which refers to context_ref.
    name_individual_segment =
      REXML::XPath.first(@doc, "//#{@namespaces[:ix]}:nonNumeric" \
        "[@name='#{@namespaces[:core]}:NameIndividualSegment'" \
        " and @contextRef='#{context_ref}']")
    return name_individual_segment.text unless name_individual_segment.nil?

    # Retrieve an element of explicitMember for finding the pair.
    context = REXML::XPath.first(@doc, "//#{@namespaces[:xbrli]}:context[@id='#{context_ref}']")
    return nil if context.nil?

    explicit_member = REXML::XPath.first(context, "#{@namespaces[:xbrli]}:entity/#{@namespaces[:xbrli]}:segment/" \
      "#{@namespaces[:xbrldi]}:explicitMember")
    return nil if explicit_member.nil?

    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    REXML::XPath.each(@doc, "//#{@namespaces[:xbrldi]}:explicitMember[contains(text(), '#{explicit_member.text}')]/" \
      "parent::#{@namespaces[:xbrli]}:segment/parent::#{@namespaces[:xbrli]}:entity/parent::#{@namespaces[:xbrli]}:context" \
      "[not(@id='context_ref')]") do |element|
      context_ref1 = element.attributes['id']
      name_individual_segment =
        REXML::XPath.first(@doc, "//#{@namespaces[:ix]}:nonNumeric" \
          "[@name='#{@namespaces[:core]}:NameIndividualSegment'" \
          " and @contextRef='#{context_ref1}']")
      return name_individual_segment.text unless name_individual_segment.nil?
    end
    nil
  end

  def get_accounts_from_elements(account_name, account_numeric)
    accounts = []
    xpath = case @filename
            when /\.html$/
              if @namespaces.key?(:ix)
                "//#{@namespaces[:ix]}:nonFraction[@name='#{account_name}']"
              end
            when /\.xml$/
              "//#{account_name}"
            end
    return accounts if xpath.nil?

    @doc.each_element(xpath) do |account|
      context_ref = account.attributes['contextRef']
      context = @contexts[context_ref]
      if context.nil?
        warn 'Invalid formant: no contextRef attr'
        break
      end

      name_individual_segment =
        (get_name_individual_segment(context_ref) if @filename =~ /\.html$/)
      data = if account_numeric
               unit_ref = account.attributes['unitRef']
               scale = account.attributes['scale']
               sign = account.attributes['sign']
               apply_scale_and_sign(account.text, scale, sign)
             else
               account.text
             end
      period = context[:period]

      accounts.push([data, @units[unit_ref], period, name_individual_segment])
    end
    accounts
  end

  def parse
    case @filename
    when /\.html$/
      parse_html
    when /\.xml$/
      parse_xml
    end
    @parsed
  end

  def parse_html
    load_namespaces
    load_contexts
    load_units

    company_number =
      REXML::XPath.first(@doc,
                         "//#{@namespaces[:ix]}:nonNumeric" \
                         "[@name='#{@namespaces[:bus]}:UKCompaniesHouseRegisteredNumber']")
    @company_number =
      unless company_number.nil?
        if company_number.has_text?
          company_number.text
        else
          # text may be located under ix's child elements
          company_number.elements.collect(&:text).compact[0]
        end
      end
    @parsed = true
  end

  def parse_xml
    # open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
    load_namespaces
    load_contexts
    load_units

    company_number =
      REXML::XPath.first(@doc,
                         "//#{@namespaces[:ae]}:CompaniesHouseRegisteredNumber")
    @company_number = company_number.text unless company_number.nil?
    @parsed = true
  end

  def get_accounts
    return [] unless @parsed

    case @filename
    when /\.html$/
      get_accounts_from_html
    when /\.xml$/
      get_accounts_from_xml
    end
  end

  def get_accounts_from_html
    account_names = ["#{@namespaces[:core]}:TurnoverRevenue",
                     "#{@namespaces[:core]}:WagesSalaries",
                     "#{@namespaces[:core]}:SocialSecurityCosts",
                     "#{@namespaces[:core]}:PensionOtherPost-employmentBenefitCostsOtherPensionCosts",
                     "#{@namespaces[:core]}:RetainedEarningsAccumulatedLosses",
                     "#{@namespaces[:core]}:FixedAssets",
                     "#{@namespaces[:core]}:DividendsPaid"]

    account_names.each do |account_name|
      _namespace, account_name2 = *account_name.split(/\:/)
      accounts = get_accounts_from_contexts(account_name)
      accounts.each do |account|
        puts [@company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end

      accounts = get_accounts_from_elements(account_name)
      accounts.each do |account|
        puts [@company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end
    end
  end

  def get_accounts_from_xml
    account_infos = []

    # account_names.push("#{@namespaces[:pt]}:GrossDividendPaymentAllShares")
    # account_names.push("#{@namespaces[:pt]}:CashBankInHand")
    account_infos.push(["#{@namespaces[:ae]}:AccountsAreInAccordanceWithSpecialProvisionsCompaniesActRelatingToSmallCompanies", false])

    account_infos.each do |account_info|
      account_name = account_info.shift
      account_numeric = account_info.shift
      namespace, account_name2 = *account_name.split(/\:/)

      accounts = []
      accounts << get_accounts_from_contexts(account_name, account_numeric)
      accounts << get_accounts_from_elements(account_name, account_numeric)
      accounts.flatten!(1)

      warn "not small?: #{@company_number}" if accounts.empty?
      accounts.each do |account|
        puts [@company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end
    end
  end
end

class UKAccounts
  def initialize(target_dir)
    @target_dir = target_dir
  end

  def read_file(zipname, file_pattern)
    Zip::ZipFile.open(zipname) do |zipfile|
      zipfile.each do |entry|
        unless file_pattern.nil?
          next unless file_pattern.match(entry.name)
        end
        p entry.name
        data = zipfile.read(entry.name)
        document = UKAccountsDocument.new(data, entry.name)
        document.parse
        document.get_accounts
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
