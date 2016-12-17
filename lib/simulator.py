'''
Created on Dec 13, 2016
 
@author: JulianYGao
'''
"""
The simulator for running path generation
"""
from utils import *
 
class Simulator(object):
    """

    """
    def __init__(self, rdd, start_time, available_time, grid_factor):

        self.start_time = start_time
        self.end_time = get_state_time_stamp(start_time, available_time)[0]
        self.hotspots = sort_hotspots(rdd)
        self.grid_factor = grid_factor
        self.grids = rdd.map(lambda (k, v): k[0]).distinct().collect()
        self.database = rdd.collectAsMap()
        # Boundary should not be a problem here...

    def profit_estimation(self, policy, start_loc, iters=20):
        """
        Note that initial state should match with the input start state
        for MDP value iteration as well
        """
        simulated_paths = []
        general_profit, general_dist = 0.0, 0.0

        for _ in range(iters):

            # randomly start from a set of POI's for testing
            curr_state = (gridify(start_loc[0], start_loc[1], self.grid_factor), self.start_time)
            total_profit, total_dist = 0.0, 0.0
            path_info = {'path': [(curr_state, 0.0, 0.0)], 'profit': (0.0, 0.0)}

            while get_state_time(curr_state[1]) < self.end_time:
                info = self.database.get((curr_state[0], 
                    get_state_time_hr(curr_state[1])), None)
                if info:
                    distance_dist, _, pay_dist, cruise_time, v, pickup_prob, \
                        dropoff_map = info
                else:
                    v, pickup_prob = 0.18, 0.0
                # Now it's time to make the decision
                pickup = np.random.choice([0, 1], p=[1 - pickup_prob, pickup_prob]) # p=
                if pickup:
                    # Second, if there is passenger when leaving current location
                    # Just sample a random place from database
                    location_list, location_prob = zip(*dropoff_map.items())
                    dropoff_location = location_list[np.random.choice(range(len(location_list)), 
                        p=location_prob)]
                    travel_distance = manhattan_distance(dropoff_location, curr_state[0])
                    travel_time = cruise_time + travel_distance / v
                    next_state = (dropoff_location,
                        get_state_time_stamp(curr_state[1], travel_time)[2])
                    # p_travel_distance * (pay_dist[0] / distance_dist[0])
                    bundle = (fare_estimation(distance_dist, pay_dist, 
                        travel_distance), travel_distance, next_state)
                else:
                    # Create two rewards and sample by chance. First,
                    # if didn't pickup passenger when going to the next location
                    
                    # What if not in policy? Assume this place definite has no passenger
                    # So driver can just heading to the next state
                    next_non_pickup_loc = policy.get(curr_state, 
                        self._eval_hotspots(curr_state))
                    travel_distance = manhattan_distance(next_non_pickup_loc, curr_state[0])
                    travel_time = cruise_time +  travel_distance / v
                    next_state = (next_non_pickup_loc, 
                        get_state_time_stamp(curr_state[1], travel_time)[2])
                    # Reward in form of (money, distance) pair
                    bundle = (0.0, travel_distance, next_state)

                # Move on to that state
                curr_state = bundle[2]
                total_profit += bundle[0]
                total_dist += bundle[1]
                path_info['path'].append((curr_state, bundle[0], bundle[1]))
            path_info['profit'] = (total_profit, total_dist)
            simulated_paths.append(path_info)
            general_profit += total_profit / iters
            general_dist += total_dist / iters

        return simulated_paths, (general_profit, general_dist)

    def get_states(self, berserk=1):
        time, state = [], []
        for t_curr in time_range(get_state_time(self.start_time), self.end_time):
            time.append(t_curr.strftime('%H:%M'))
        # If totally random, enumerate all grids inside boundaries in order to 
        # avoid hotspot eval in unseen cases
        grids = enumerate_grids((-74.021611, -73.742833, 40.616669, 
            40.886116), self.grid_factor * 0.00111) if berserk else self.grids

        # Then merge all grids and possible times
        for g in grids:
            state += zip([g] * len(time), time)  
        return state

    def _eval_hotspots(self, state):
        curr_loc, curr_hr = state[0], get_state_time_hr(state[1])
        best_loc = max((self._eval_profit(curr_loc, loc, curr_hr), 
            loc) for loc in self.hotspots[curr_hr])[1]
        return best_loc

    def _eval_profit(self, curr_loc, location, hr):
        distance_dist, time_dist, pay_dist, cruise_time, v, pickup_prob, _ = self.database[(location, hr)]
        trip_time = manhattan_distance(location, curr_loc) / v + cruise_time
        return get_state_reward(trip_time, distance_dist, time_dist, pay_dist, pickup_prob)


