"""
A trivial hard coded oracle performance
"""

def oracle_test(test_data):
	"""
	100% correctly reports if the next day's opening DJIA will 
	be higher than today's closing DJIA
	"""
	res = {
		'2016-03-21': -1, '2016-03-22': 1, '2016-03-23': -1, \
		'2016-03-24': 1, '2016-03-08': 1, '2016-03-09': 1, \
		'2016-03-28': -1, '2016-03-29': 1, '2016-03-04': -1, \
		'2016-03-02': -1, '2016-03-03': 1, '2016-03-01': -1, \
		'2016-03-07': -1, '2016-03-18': -1, '2016-03-31': -1, \
		'2016-03-30': -1, '2016-03-15': -1, '2016-03-14': -1, \
		'2016-03-17': 1, '2016-03-16': -1, '2016-03-11': -1, '2016-03-10': 1
		}
	date = test_data.keys()[0].split(' ')[0]

	if res[date] > 0:
		print date,'tomorrow\'s opening DJIA will be higher than today\'s closing DJIA'
	else:
		print date,'tomorrow\'s opening DJIA will be lower than today\'s closing DJIA'
	return res[date]
