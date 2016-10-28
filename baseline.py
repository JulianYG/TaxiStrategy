from datetime import datetime
"""
A trivial intuitive baseline case for prediction
"""

def base_test(test_data):
	"""
	A really simple test case that only extract features
	to calculate average trip fare for prediction, as a baseline
	"""
	fmt = '%H:%M:%S'
	total_time = 0
	total_charge = 0.0
	for row in test_data.values():
		total_charge += float(row[-1])
		pick_up_time = row[0].split(' ')[1]
		drop_off_time = row[1].split(' ')[1]
		total_time += (datetime.strptime(drop_off_time, \
			fmt) - datetime.strptime(pick_up_time, fmt)).seconds
	avg_rate = total_charge / total_time
	date = test_data.keys()[0].split(' ')[0]

	if avg_rate <= 0.025:
		print date, 'tomorrow\'s opening DJIA will be lower than today\'s closing DJIA'
		return -1
	else:
		print date, 'tomorrow\'s opening DJIA will be higher than today\'s closing DJIA'
		return 1
