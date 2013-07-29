
require 'sinatra'
require 'sequel'
require 'json'

BATCH_SIZE = 1_000

DB = Sequel.sqlite
DB.create_table :quotes do
  primary_key :id
  String :origin
  String :text
end

KNOWN_ORIGINS = []

# read quotes into a memory db
Dir.glob(File.join(File.dirname(__FILE__), 'quotes',  "[^_]*.txt")) do |filename|
  origin = File.basename(filename, '.txt')
  KNOWN_ORIGINS << origin

  file = File.open(filename)
  data = []

  puts "-- reading #{origin} quotes"

  # count lines
  count = file.lines.count

  file.rewind
  file.each_line.each_with_index do |l, i|
    l.strip!
    next if l.start_with?('#') || l.empty?
    data.push({ origin: origin, text: l })

    if (i % BATCH_SIZE) == 0
      print "#{i} of #{count} ..."
      print "\r"
      $stdout.flush

      DB[:quotes].multi_insert(data, :commit_every => BATCH_SIZE)
      data.clear
    end
  end
  file.close

  DB[:quotes].multi_insert(data)
  puts "   done (#{DB[:quotes].where(origin: origin).count} entries)"
end

error 400..499 do
  content_type :json
  { error: 'your fault' }.to_json
end

error 500..599 do
  content_type :json
  { error: 'my fault' }.to_json
end

get '/quotes/:name' do |name|
  content_type :json
  halt 404 unless KNOWN_ORIGINS.include?(name)

  results = DB[:quotes].where(origin: name)

  count = results.count
  item = results.limit(1, rand(count)).first

  { origin: item[:origin], text: item[:text] }.to_json
end
