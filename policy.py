from lib.utils import *


def generate_oracle_policy():
	return

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



