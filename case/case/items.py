# -*- coding: utf-8 -*-

# Items: 
# --application_info: includes school and personal information
# 	of applicant 
# --pid: user id of applicant 
# --url: web address of current post 
# --reply: number of replies of current post
# --view: number of views of current post

import scrapy

class Report(scrapy.Item):
	
	time = scrapy.Field()
	title = scrapy.Field()
	post = scrapy.Field()
	