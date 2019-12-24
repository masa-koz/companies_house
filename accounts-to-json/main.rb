# frozen_string_literal: true

require 'csv'
require 'json'
require 'zip/zip'
require 'rexml/document'
require 'rexml/xpath'
require 'parallel'
require 'pstore'

class UKAccountsDocument
  def initialize(data, filename, output, worker_id, debug)
    @doc = REXML::Document.new(data)
    @filename = filename
    @output = output
    @ns = {}
    @parsed = false
    @worker_id = worker_id
    @debug = debug

    if /\_(\d{8})\_(\d{8})\.html$/.match(filename)
      @registered_number = $LAST_MATCH_INFO[1]
      @filing_date = $LAST_MATCH_INFO[2]
    end
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

  def convert_units
    prefix =
      if @ns.key?(:xbrli)
        "#{@ns[:xbrli]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}unit") do |unit|
      unit_hash = {}
      id = unit.attributes['id']
      unit_hash[:id] = id

      measure = REXML::XPath.first(unit, "#{prefix}measure")
      unless measure.nil?
        text = measure.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        unit_hash[:measure] = text
      end

      json = { registered_number: @registered_number, filing_date: @filing_date, unit: unit_hash }
      @output.puts json.to_json
      @output.flush
    end
  end

  def convert_contexts
    prefix =
      if @ns.key?(:xbrli)
        "#{@ns[:xbrli]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}context") do |context|
      context_hash = {}

      id = context.attributes['id']
      context_hash[:id] = id

      identifier = REXML::XPath.first(context, "#{prefix}entity/#{prefix}identifier/")
      unless identifier.nil?
        text = identifier.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        context_hash[:identifier] = text
      end

      explicit_member = REXML::XPath.first(context, "#{prefix}entity/#{prefix}segment/#{@ns[:xbrldi]}:explicitMember")
      unless explicit_member.nil?
        text = explicit_member.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        context_hash[:explicit_member] = text
      end

      start_date = REXML::XPath.first(context, "#{prefix}period/#{prefix}startDate")
      unless start_date.nil?
        text = start_date.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        context_hash[:start_date] = text
      end

      end_date = REXML::XPath.first(context, "#{prefix}period/#{prefix}endDate")
      unless end_date.nil?
        text = end_date.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        context_hash[:end_date] = text
      end

      instant = REXML::XPath.first(context, "#{prefix}period/#{prefix}instant")
      unless instant.nil?
        text = instant.get_text
        text = text.value.gsub(/\s/, '') unless text.nil?
        context_hash[:instant] = text
      end

      forever = REXML::XPath.first(context, "#{prefix}period/#{prefix}forever")
      context_hash[:forever] = '' unless forever.nil?

      json = { registered_number: @registered_number, filing_date: @filing_date, context: context_hash }
      @output.puts json.to_json
      @output.flush
    end
  end

  def convert_ixs
    prefix =
      if @ns.key?(:ix)
        "#{@ns[:ix]}:"
      else
        ''
      end

    REXML::XPath.each(@doc, "//#{prefix}nonNumeric") do |non_numeric|
      non_numeric_hash = {}

      non_numeric_hash[:name] = non_numeric.attributes['name']
      non_numeric_hash[:contextRef] = non_numeric.attributes['contextRef']
      text = non_numeric.get_text
      text = text.value.gsub(/\s/, '') unless text.nil?
      non_numeric_hash[:text] = text

      json = { registered_number: @registered_number, filing_date: @filing_date, nonNumeric: non_numeric_hash }
      @output.puts json.to_json
      @output.flush
    end

    REXML::XPath.each(@doc, "//#{prefix}nonFraction") do |non_fraction|
      non_fraction_hash = {}

      non_fraction_hash[:name] = non_fraction.attributes['name']
      non_fraction_hash[:contextRef] = non_fraction.attributes['contextRef']
      non_fraction_hash[:unitRef] = non_fraction.attributes['unitRef']
      non_fraction_hash[:decimals] = non_fraction.attributes['decimals']
      non_fraction_hash[:format] = non_fraction.attributes['format']
      non_fraction_hash[:scale] = non_fraction.attributes['scale']
      non_fraction_hash[:sign] = non_fraction.attributes['sign']
      text = non_fraction.get_text
      text = text.value.gsub(/\s/, '') unless text.nil?
      non_fraction_hash[:text] = text

      json = { registered_number: @registered_number, filing_date: @filing_date, nonFraction: non_fraction_hash }
      @output.puts json.to_json
      @output.flush
    end

    # ix:denominator
    # ix:fraction
    # ix:numerator
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
      warn "[Worker#{@worker_id.nil? ? 0 : @worker_id}]#{$ERROR_INFO.full_message}"
      open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
      exit if @debug
    end
    @parsed
  end

  def parse_html
    # open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }

    load_namespace

    convert_units
    convert_contexts
    convert_ixs

    @parsed = true
  end

  def parse_xml
    # open(@filename, 'w') { |out| @doc.write(output: out, indent: 2) }
    load_namespace
    load_contexts
    load_units

    @parsed = true
  end
end

class UKAccounts
  def initialize(target_dir, worker_id, debug)
    @target_dir = target_dir
    @worker_id = worker_id
    @target_subdir = if worker_id.nil?
                       '.'
                     else
                       worker_id.to_s
                     end
    csvfilename = if worker_id.nil?
                    "accounts_#{Time.now.tv_sec}.json"
                  else
                    "accounts_#{worker_id}_#{Time.now.tv_sec}.json"
    end
    @output = open(csvfilename, 'w')
    @debug = debug
  end

  def read_file(zipname, file_pattern)
    db = PStore.new("#{zipname}.db")
    processed = db.transaction { db['processed'] }
    processed = 0 if processed.nil?
    warn "[Worker#{@worker_id.nil? ? 0 : @worker_id}]zipfile: #{zipname}, processed: #{processed}"
    begin
    Zip::ZipFile.open(zipname) do |zipfile|
      number = zipfile.entries.size
      zipfile.each_with_index do |entry, i|
        if i > processed
          warn "[Worker#{@worker_id.nil? ? 0 : @worker_id}](#{i + 1}/#{number}): #{entry.name}"
        else
          warn "[Worker#{@worker_id.nil? ? 0 : @worker_id}][skipped](#{i + 1}/#{number}): #{entry.name}"
          next
        end
        unless file_pattern.nil?
          next unless file_pattern.match(entry.name)
        end
        data = zipfile.read(entry.name)
        document = UKAccountsDocument.new(data, entry.name, @output, @worker_id, @debug)
        document.parse
        db.transaction { db['processed'] = i }
        STDERR.flush
      end
    end
    rescue StandardError
      warn "[Worker#{@worker_id.nil? ? 0 : @worker_id}]: In processing #{zipname}: #{$ERROR_INFO.full_message}"
    end
  end

  def read_dir(file_pattern, zip_pattern)
    Dir.chdir(@target_dir) do
      Dir.chdir(@target_subdir) do
        Dir.glob('*.zip') do |zipname|
          unless zip_pattern.nil?
            next unless zip_pattern.match(zipname)
          end
          read_file(zipname, file_pattern)
        end
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

Parallel.each((1..12).to_a) do |i|
  accounts = UKAccounts.new(dirname, i, false)
  accounts.read_dir(file_pattern, zip_pattern)
end
