# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import sys
import codecs
from bs4 import BeautifulSoup as BSoup
reload(sys)
sys.setdefaultencoding('utf-8')

class CasePipeline(object):

    def __init__(self):
        self.file = codecs.open('case_data.json', mode='wb', encoding='utf-8')

    def process_item(self, item, spider):
        
        line = ''
        # Initialize the info data structure
        info_dic = {
            # logistics
            'pid': item['pid'], 'url': item['url'], 'region': item['region'],
            # level of interests
            'reply': item['reply'], 'view': item['view'], 
            # applicant personal background
            'bg_info': None, 
            # applicant application info (list of dictionaries)
            'app_info': []
        }
        content = item['application_info']
        # rely on this order since personal information is always the last one
        school_tables, personal_table = [], []
        
        for table in content:
            miso_soup = BSoup(table, "lxml")
            tag = miso_soup.caption
            if 'offer' in tag.get_text().strip():
                school_tables.append(table)
            if tag.get_text().strip() == '个人情况':
                personal_table.append(table)

        for school in school_tables:
            school_dic = self._parse_school_info(school, self._school_factory())
            info_dic['app_info'].append(school_dic)

        info_dic['bg_info'] = self._parse_personal_info(personal_table[0], self._person_factory())
        
        line += json.dumps(info_dic, ensure_ascii=False) + '\n'
        self.file.write(line)

    def close_spider(self, spider):
        self.file.close()

    def _school_factory(self):
        return {
            'school': '', 'degree': '', 'major': '', 'result': '', 
            'enrollment_yr': '', 'enrollment_sms': '', 'notification_t': ''
        }

    def _person_factory(self):
        return {
            'test_score': {'SAT': '', 'TOEFL': '', 'IELTS': '', 'ACT': '', 
                'GRE': '', 'LSAT': '', 'GMAT': '', 'MCAT': '', 'sub': ''}, 
            'current_school': '', 'current_major': '', 'gpa': '', 
            'notes': ''
        }

    def _parse_school_info(self, school_info, dic):

        soup = BSoup(school_info, "lxml")
        for th, td in zip(soup.find_all('th'), soup.find_all('td')):
            if th.get_text() == '申请学校:':
                dic['school'] = td.get_text().strip()
            if th.get_text() == '学位:':
                dic['degree'] = td.get_text().strip()
            if th.get_text() == '专业:':
                dic['major'] = td.get_text().strip()
            if th.get_text() == '申请结果:':
                dic['result'] = td.get_text().strip()
            if th.get_text() == '入学年份:':
                dic['enrollment_yr'] = td.get_text().strip()
            if th.get_text() == '入学学期:':
                dic['enrollment_sms'] = td.get_text().strip()
            if th.get_text() == '通知时间:':
                dic['notification_t'] = td.get_text().strip()
        return dic

    def _parse_personal_info(self, personal_info, dic):

        soup = BSoup(personal_info, "lxml")
        for th, td in zip(soup.find_all('th'), soup.find_all('td')):
            if th.get_text() == 'IELTS:':
                dic['test_score']['IELTS'] = td.get_text().strip()
            if th.get_text() == 'TOEFL:':
                dic['test_score']['TOEFL'] = td.get_text().strip()
            if th.get_text() == 'GRE:':
                dic['test_score']['GRE'] = td.get_text().strip()
            if th.get_text() == 'SAT:':
                dic['test_score']['SAT'] = td.get_text().strip()
            if th.get_text() == 'GMAT:': 
                dic['test_score']['GMAT'] = td.get_text().strip()
            if th.get_text() == 'ACT:': 
                dic['test_score']['ACT'] = td.get_text().strip()
            if th.get_text() == 'LSAT:': 
                dic['test_score']['LSAT'] = td.get_text().strip()
            if th.get_text() == 'MCAT:': 
                dic['test_score']['MCAT'] = td.get_text().strip()
            if th.get_text() == 'sub:': 
                dic['test_score']['sub'] = td.get_text().strip()
            if th.get_text() == '本科专业:': 
                dic['current_major'] = td.get_text().strip()
            if th.get_text() == '其他说明:': 
                dic['notes'] = td.get_text().strip()
            if th.get_text() == '本科专业:': 
                dic['current_major'] = td.get_text().strip()
            if th.get_text() == '本科学校档次:': 
                dic['current_school'] = td.get_text().strip()
            if th.get_text() == '本科成绩和算法、排名:': 
                dic['gpa'] = td.get_text().strip()
        return dic
