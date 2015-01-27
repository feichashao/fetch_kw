#!/usr/bin/ruby -w
# This script fetch keywords and corresponding urls & titles recursively from baidu.com
# Usage: kw_fetch.rb [init_kw]
# Author: feichashao@gmail.com
# Known issues:
#	1. fetching keywords from suggestion.baidu.com encounters a
#	string encoding problem. Don't know how to fix yet.
#	(Thus fetch_sugkw method is disabled)
#

require "socket"
require "mysql"
require "set"
require "thread"

# Constants 
HOST = 'www.baidu.com'
PORT = 80
PATH = "/s?wd="
INIT_KW = "肥叉烧"

SUG_HOST = 'suggestion.baidu.com'	# Suguestion keyword relative.
SUG_PORT = '80'
SUG_PATH = '/su?wd='

MAX_KEYWORDS = 100	# How many keywords are going to fetch.
MAX_TO_VISIT = 150 	# Max keywords in the to_visit queue.
			# MAX_TO_VISIT should be larger than MAX_KEYWORDS

MAX_DEPTH = 6		# Not yet used

# Database
MYSQL_HOSTNAME = 'localhost'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'redhat'
MYSQL_DATABASE = 'ruby'
KW_TBL_NAME = 'keywords' 	# Name of table which stores keywords
URL_TBL_NAME = 'urls'		# Name of table which stores titles and urls


# Variables
@to_visit = Array.new 	# keywords to be visit. 
@visited = Set.new	# keywords visited.
@mutex = Mutex.new	# Multi-thread sync.


# Init method
# Call this method before starting any fetch thread.
def init
	# Create tables
	db = Mysql.new(MYSQL_HOSTNAME, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)
 	db.query( "CREATE TABLE IF NOT EXISTS #{KW_TBL_NAME}( kw_id int  NOT NULL PRIMARY KEY AUTO_INCREMENT, keyword varchar(50)  );" )
 	db.query( "CREATE TABLE IF NOT EXISTS #{URL_TBL_NAME}( url_id int NOT NULL PRIMARY KEY AUTO_INCREMENT,  keyword varchar(50), title varchar(100), url varchar(120)  );" )
	db.close
	
	# Init keyword
	if ARGV.respond_to?("[]") then
		@to_visit << ARGV[0]
	else
		@to_visit << INIT_KW
	end

end

#
# fetch
# Get web content from baidu.com using queued keywords continuously
# and hand over the content to content_handle method.
#
def fetch
	# Open MySQL connection.	
	db = Mysql.new(MYSQL_HOSTNAME, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)
	# If not yet visited enouth keywords, fetch more (URLs and relative kws)
	while @visited.length < MAX_KEYWORDS do
		kw = ""
		got_kw = false
		while not got_kw do
			@mutex.lock
			kw = @to_visit.shift
			@mutex.unlock
			if kw == nil then
				next
			end
			@mutex.lock
			if not @visited.include?(kw)
				got_kw = true
				@visited << kw
			end
			@mutex.unlock
		end
		
		# Open socket
		# TODO Exception handling.
		socket = TCPSocket.open(HOST, PORT)
		# Read web content.
		request = "GET #{PATH}#{kw} HTTP/1.0\r\n\r\n"
		socket.print(request)
		response = socket.read
		headers,body = response.split("\r\n\r\n", 2)
		socket.close
		print "DEBUG-FETCHING #{kw}\n"
		content_handle(kw,body,db)
	end
	# Close MySQL connection.
	db.close
end

# content_handle
# Handle web content
# 1. Fetch relative keywords;
# 2. Fetch URLs.
def content_handle(kw,content,db)

	# Put kw into Database
	db_result = db.query("INSERT INTO #{KW_TBL_NAME}(keyword) VALUES(\"#{kw}\")")
	
	# Get more keywords
	result_div =  /<div id=\"rs\">(.*?)<\/div><div id=/m.match(content) # Match <div id = "rs">
	if not result_div.respond_to?("[]") then return end
	result_kw = result_div[1].scan(/<a.*?>(.*?)<\/a>/m)		# Match keywords
	# Put keywords into to_visit.
	if result_kw.respond_to?("each") and @to_visit.length <= MAX_TO_VISIT 
        	result_kw.each do |rkw|
			@mutex.lock
                	@to_visit << rkw
			@mutex.unlock	
			puts "Got kw: #{rkw}\n"
        	end
	end

	# Get search results, URLs and titles
	# and put them into database
	urls_h3 = content.scan(/<h3 class=\"t.*?>(.*?)<\/h3>/m)    # Match all <h3> tags.
	if urls_h3.respond_to?("each")
        	urls_h3.each do |url_h3|
                	url_match = /(http:\/\/.*?)\"/.match(url_h3[0])
			if not url_match.respond_to?("[]") then next end
			url = url_match[1]
                	title_match = />(.*?)<\/a>/.match(url_h3[0])
               		if not title_match.respond_to?("[]") then next end
			title = title_match[1].gsub("<em>","").gsub("</em>","")
			print title
                	print "\n"
                	print url
                	print "\n\n"
			# Put url into database
			title = Mysql.escape_string(title);
			url = Mysql.escape_string(url);
			db.query( "INSERT INTO #{URL_TBL_NAME}(keyword, title, url) VALUES(\'#{kw}\',\'#{title}\',\'#{url}\')" )
        	end
	end	

end

# fetch_sugkw method
# FIXME seems to be encoding problem.
#

# def fetch_sugkw(kw)
#	request = "GET #{SUG_PATH}#{kw} HTTP/1.0\r\n\r\n"
#
#	socket = TCPSocket.open(SUG_HOST,SUG_PORT)
#	socket.print(request)
#	response = socket.read
#
#	headers,body = response.split("\r\n\r\n", 2)
#
#	print body
#	print "\n"
#
#	su_match = /s\:\[(.*?)\]/.match(body)
#	print su_match[1]
#	print "\n"
#
#	socket.close

# end

#### Program starts below ####
#
# Call init
init

# Multi-thread
t1 = Thread.new{fetch()}
t2 = Thread.new{fetch()}
t3 = Thread.new{fetch()}
t4 = Thread.new{fetch()}
t5 = Thread.new{fetch()}
t1.join
t2.join
t3.join
t4.join
t5.join


