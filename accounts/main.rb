# frozen_string_literal: true

require 'zip/zip'
require 'rexml/document'
require 'rexml/xpath'
require 'csv'

class UKAccountsDocument
  def initialize(data, filename, debug)
    @doc = REXML::Document.new(data)
    @filename = filename
    @ns = {}
    @units = {}
    @contexts = {}
    @parsed = false
    @debug = debug
  end

  def load_namespace
    namespaces = case @filename
                 when /\.html$/
                   REXML::XPath.first(@doc, '/html').namespaces
                 when /\.xml$/
                   REXML::XPath.first(@doc, '/*:xbrl').namespaces
                 else
                   return
    end

    namespaces.each do |local, namespace|
      case namespace
      when %r{^http\://www\.xbrl\.org/[^/]+/instance$}
        @ns[:xbrli] = local
      when %r{^http\://xbrl\.org/[^/]+/xbrldi$}
        @ns[:xbrldi] = local
      when %r{^http\://www\.xbrl\.org/[^/]+/inlineXBRL$}
        @ns[:ix] = local
      when %r{^http\://xbrl\.frc\.org\.uk/fr/[^/]+/core$}
        @ns[:core] = local
      when %r{^http\://xbrl\.frc\.org\.uk/cd/[^/]+/business$}
        @ns[:bus] = local
      when %r{^http\://www\.companieshouse\.gov\.uk/ef/xbrl/uk/fr/gaap/ae/[^/]+$}
        @ns[:ae] = local
      when %r{^http\://www\.xbrl\.org/uk/fr/gaap/pt/[^/]+$}
        @ns[:pt] = local
      end
    end
  end

  def load_units
    prefix =
      if @ns.key?(:xbrli)
        "#{@ns[:xbrli]}:"
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
      if @ns.key?(:xbrli)
        "#{@ns[:xbrli]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}context") do |context|
      id = context.attributes['id']
      @contexts[id] = {}
      explicit_member = REXML::XPath.first(context, "#{prefix}entity/#{prefix}segment/" \
        "#{prefix}explicitMember")
      unless explicit_member.nil?
        @contexts[id][:name] = explicit_member.text.gsub(/\s/, '')
        @contexts[id][:dimension] = explicit_member.attributes['dimension']
      end

      period = REXML::XPath.first(context, "#{prefix}period")
      period = parse_period(period)
      @contexts[id][:period] = period
    end
  end

  def parse_period(element)
    prefix =
      if @ns.key?(:xbrli)
        "#{@ns[:xbrli]}:"
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

  def full_accounts?
    xpath = "//#{@ns[:xbrldi]}:explicitMember[contains(text(), '#{@ns[:bus]}:FullAccounts')]"
    !REXML::XPath.first(@doc, xpath).nil?
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
    return accounts unless @ns.key?(:xbrldi)

    "parent::#{@ns[:xbrli]}:segment/parent::#{@ns[:xbrli]}:entity/parent::#{@ns[:xbrli]}:context"
    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    REXML::XPath.each(@doc, "//#{@ns[:xbrldi]}:explicitMember[contains(text(), '#{account_name}')]/" \
      "parent::#{@ns[:xbrli]}:segment/parent::#{@ns[:xbrli]}:entity/parent::#{@ns[:xbrli]}:context") do |context|
      context_ref = context.attributes['id']
      account = REXML::XPath.first(@doc, "//#{@ns[:ix]}:nonFraction[@contextRef='#{context_ref}']")
      next if account.nil?

      data = if account_numeric
               unit_ref = account.attributes['unitRef']
               scale = account.attributes['scale']
               sign = account.attributes['sign']
               apply_scale_and_sign(account.text, scale, sign)
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
      REXML::XPath.first(@doc, "//#{@ns[:ix]}:nonNumeric" \
        "[contains(@name,'NameIndividualSegment')" \
        " and @contextRef='#{context_ref}']")
    unless name_individual_segment.nil?
      return name_individual_segment.text.gsub(/\s/, '')
    end

    # Retrieve an element of explicitMember for finding the pair.
    context = REXML::XPath.first(@doc, "//#{@ns[:xbrli]}:context[@id='#{context_ref}']")
    return nil if context.nil?

    explicit_member = REXML::XPath.first(context, "#{@ns[:xbrli]}:entity/#{@ns[:xbrli]}:segment/" \
      "#{@ns[:xbrldi]}:explicitMember")
    return nil if explicit_member.nil?

    # remove non-letter such as newline.
    explicit_member = explicit_member.text.gsub(/\s/, '')
    # Try to find an element of NameIndividualSegment which refers to the context of a member of the pair.
    xpath = "//#{@ns[:xbrldi]}:explicitMember[contains(text(),\'#{explicit_member}')]/" \
    "parent::#{@ns[:xbrli]}:segment/parent::#{@ns[:xbrli]}:entity/parent::#{@ns[:xbrli]}:context" \
    "[not(@id='context_ref')]"
    REXML::XPath.each(@doc, xpath) do |element|
      context_ref1 = element.attributes['id']
      name_individual_segment =
        REXML::XPath.first(@doc, "//#{@ns[:ix]}:nonNumeric" \
          "[contains(@name,'NameIndividualSegment')" \
          " and @contextRef='#{context_ref1}']")
      unless name_individual_segment.nil?
        return name_individual_segment.text.gsub(/\s/, '')
      end
    end
    nil
  end

  def get_accounts_from_elements(account_name, account_numeric)
    accounts = []
    xpath = case @filename
            when /\.html$/
              if @ns.key?(:ix)
                "//#{@ns[:ix]}:nonFraction[@name='#{account_name}']"
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
               apply_scale_and_sign(account.text.gsub(/\s/, ''), scale, sign)
             else
               account.text.gsub(/\s/, '')
             end
      period = context[:period]

      accounts.push([data, @units[unit_ref], period, name_individual_segment])
    end
    accounts
  end

  def parse
    begin
      case @filename
      when /\.html$/
        parse_html
      when /\.xml$/
        parse_xml
      end
    rescue StandardError
      warn $ERROR_INFO.full_message
      open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
      exit if @debug
    end
    @parsed
  end

  def parse_html
    # open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }

    load_namespace
    load_contexts
    load_units

    company_number =
      REXML::XPath.first(@doc,
                         "//#{@ns[:ix]}:nonNumeric[contains(@name,'UKCompaniesHouseRegisteredNumber')]")
    @company_number =
      unless company_number.nil?
        if company_number.has_text?
          company_number.text.gsub(/\s/, '')
        else
          # text may be located under ix's child elements
          company_number.elements.collect(&:text).compact[0]
        end
      end
    warn "UKCompaniesHouseRegisteredNumber: #{@company_number}"

    @parsed = true
  end

  def parse_xml
    # open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
    load_namespace
    load_contexts
    load_units

    company_number =
      REXML::XPath.first(@doc,
                         "//#{@ns[:ae]}:CompaniesHouseRegisteredNumber")
    unless company_number.nil?
      @company_number = company_number.text.gsub(/\s/, '')
    end
    @parsed = true
  end

  def get_accounts(output)
    return [] unless @parsed

    begin
      case @filename
      when /\.html$/
        get_accounts_from_html(output)
      when /\.xml$/
        get_accounts_from_xml(output)
      end
    rescue StandardError
      warn $ERROR_INFO.full_message
      open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
      exit if @debug
    end
  end

  def get_accounts_from_html(output)
    account_infos = [["#{@ns[:core]}:TurnoverRevenue", true],
                     ["#{@ns[:core]}:WagesSalaries", true],
                     ["#{@ns[:core]}:DividendsPaid", true],
                     ["#{@ns[:core]}:FixedAssets", true],
                     ["#{@ns[:core]}:CurrentAssets", true],
                     ["#{@ns[:core]}:CashBankOnHand", true],
                     ["#{@ns[:core]}:RetainedEarningsAccumulatedLosses", true]]

    account_infos.each do |account_info|
      account_name = account_info.shift
      account_numeric = account_info.shift
      namespace, account_name2 = *account_name.split(/\:/)
      accounts = []
      accounts << get_accounts_from_contexts(account_name, account_numeric)
      accounts << get_accounts_from_elements(account_name, account_numeric)
      accounts.flatten!(1)

      accounts.each do |account|
        output.puts [@company_number, account_name2, account[3], account[0], account[1],
                     account[2][:startDate], account[2][:endDate],
                     account[2][:instant], account[2][:forever]].to_csv
        output.flush
        warn [@company_number, account_name2, account[3], account[0], account[1],
              account[2][:startDate], account[2][:endDate],
              account[2][:instant], account[2][:forever]].to_csv
      end
    end
  end

  def get_accounts_from_xml(output)
    account_infos = []

    account_infos.each do |account_info|
      account_name = account_info.shift
      account_numeric = account_info.shift
      namespace, account_name2 = *account_name.split(/\:/)

      accounts = []
      accounts << get_accounts_from_contexts(account_name, account_numeric)
      accounts << get_accounts_from_elements(account_name, account_numeric)
      accounts.flatten!(1)

      accounts.each do |account|
        output.puts [@company_number, account_name2, account[3], account[0], account[1],
                     account[2][:startDate], account[2][:endDate],
                     account[2][:instant], account[2][:forever]].to_csv
        output.flush
      end
    end
  end
end

class UKAccounts
  def initialize(target_dir, debug)
    @target_dir = target_dir
    @output = open('accounts.csv', 'w')
    @debug = debug
  end

  def read_file(zipname, file_pattern)
    warn "Zipfile: #{zipname}"
    Zip::ZipFile.open(zipname) do |zipfile|
      number = zipfile.entries.size
      zipfile.each_with_index do |entry, i|
        warn "(#{i + 1}/#{number}): #{entry.name}"
        unless file_pattern.nil?
          next unless file_pattern.match(entry.name)
        end
        data = zipfile.read(entry.name)
        document = UKAccountsDocument.new(data, entry.name, @debug)
        document.parse
        document.get_accounts(@output)
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

accounts = UKAccounts.new(dirname, true)
accounts.read_dir(file_pattern, zip_pattern)
