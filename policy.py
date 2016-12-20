from lib.utils import *

def generate_oracle_policy(states, state, rdd):
	"""
	A dynamic programming process that finds the maximum 
	sum of profits. Can be viewed as a simplified problem: 
	finding a set of non-intersecting line segments that 
	have largest sum. 
	"""
	best_score, best_action = dp_oracle(states, state, rdd)
	print best_score
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

	# Only use this when rdd is small!
	dic_store = rdd.collectAsMap()

	# Recursive case: Look up all previous states
	# Transition function: max(Q(initial_state), Q(prev_state) + R(prev, curr))
	for s in states:
		# s_info = rdd.lookup(s)
		s_info = dic_store.get(s, None)
		prev_states = [m for m in s_info] if s_info else [(0.0, initial_state)]
		
		# Now make the choice, and track the choice
		opt_cache[s], history[s] = max([(opt_cache[ps[1]] + ps[0], 
			ps[1]) for ps in prev_states])

		# Need to reverse the order since it's going back
	history = dict((v, k[0]) for k, v in history.items())
	return max(opt_cache.values()), history




