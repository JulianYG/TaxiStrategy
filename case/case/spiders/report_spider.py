# -*- coding: utf-8 -*-
import scrapy
from case.items import Report
from case.util import *
from scrapy.selector import Selector
from bs4 import BeautifulSoup as BSoup
from scrapy.selector import HtmlXPathSelector

class CaseStudySpider(scrapy.Spider):
	name = "selfReports"
	allowed_domains = ["reddit.com"]
	# Start from different regions
	start_urls = [
		"https://www.reddit.com/r/collegeresults/",
		]

	def parse(self, response):
		"""
		Simply parse the offer list page to extract links to other pages
		"""
		suimono = BSoup(response.body, "lxml")
		page_urls = suimono.find_all("a", {"rel": "nofollow next"})
		# Get url to the next page from current post page
		if page_urls:
			next_page = page_urls[0]['href']
			yield scrapy.Request(next_page, callback=self.parse_post_on_page)

	def parse_post_on_page(self, response):
		"""
		Parse the list page source to extract links to actual posts
		"""
		tonkotsu = BSoup(response.body, "lxml")
		post_urls = tonkotsu.find_all('a', {'data-event-action': 'comments'}, href=True)
		for uri in post_urls:
			# Access 25 individual posts on each page
			thread_link = uri['href']
			yield scrapy.Request(thread_link, callback=self.parse_post)
		
	def parse_post(self, response):
		"""
		Parse the actual content inside each post
		"""
		miso = BSoup(response.body, 'lxml')
		rss_url = miso.find_all('link', {"type": "application/atom+xml"}, href=True)[0]['href']
		yield scrapy.Request(rss_url, callback=self.parse_rss)

	def parse_rss(self, response):
		"""
		Parse the actual content inside the .rss
		"""
		hxs = HtmlXPathSelector(response)
		item = Report()
		item['time'] = hxs.select('//feed/entry[1]/updated/text()').extract()[0]
		item['title'] = hxs.select('//feed/entry[1]/title/text()').extract()[0]
		item['post'] = hxs.select('//feed/entry[1]/content/text()').extract()[0]
		print item['time']
		print item['title']
		print item['post']
		# yield item

	def parse_region(self, title_str):
		"""
		Parse the name of application region
		"""
		region = ' '.join((title_str.strip().split())).split(' ')[-2]
		region_str = ''
		if region == '日韩留学':
			region_str = 'JP/SK'
		if region == '新加坡留学':
			region_str = 'Singapore'
		if region == '加拿大留学申请':
			region_str = 'Canada'
		if region == '欧洲诸国留学':
			region_str = 'Europe'
		if region == '澳洲新西兰留学与移民':
			region_str = 'Oceania'
		if region == '美国留学':
			region_str = 'US'
		if region == '英国留学申请':
			region_str = 'UK'
		if region == '香港澳门台湾留学':
			region_str = 'HK/MO/TW'
		return region_str

