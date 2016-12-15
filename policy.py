from lib.utils import *

def generate_oracle_policy(states, state, rdd):
	"""
	A dynamic programming process that finds the maximum 
	sum of profits. Can be viewed as a simplified problem: 
	finding a set of non-intersecting line segments that 
	have largest sum. 
	"""
	best_action = dp_oracle(states, state, rdd)[1]
	return best_action

def generate_random_policy(states, grid_factor):

	random_policy = {}
	grid_scale = grid_factor * 0.00111
	for state in states:
		location = state[0]
		((lon0, lon1), (lat0, lat1)) = ((float(location[0][0]), 
			float(location[0][1])), (float(location[1][0]), float(location[1][1])))
		horizontal_move = random.choice([-1, 0, 1])
		vertical_move = random.choice([-1, 0, 1])
		lon0 += horizontal_move * grid_scale
		lon1 += horizontal_move * grid_scale
		lat0 += vertical_move * grid_scale
		lat1 += vertical_move * grid_scale
		random_policy[state] = ((str(lon0), str(lon1)), (str(lat0), str(lat1)))

	return random_policy

def dp_oracle(states, initial_state, rdd):

	history = {}
	# Baseline 
	opt_cache = defaultdict(float)

	# Recursive case: Look up all previous states
	# Transition function: max(Q(initial_state), Q(prev_state) + R(prev, curr))
	for s in states:
		s_info = rdd.lookup(s)
		if s_info: 
			# Choose the max s_util as well
			s_util, s_prev = max(s_info)
		else:
			s_util, s_prev = 0.0, None

		# This is to check util from initial state to here
		curr_s_info = rdd.lookup(s)
		curr_util = curr_s_info[0] if curr_s_info else 0.0

		# Now make the choice, and track the choice
		opt_cache[s] = max(opt_cache[s_prev] + s_util, curr_util)
		history[s] = initial_state if opt_cache[s] == curr_util else s_prev

		# Need to reverse the order since it's going back
		history = dict((v, k) for k, v in history.items())

	return max(opt_cache.values()), history




