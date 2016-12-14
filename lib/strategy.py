'''
Created on Dec 11, 2016

@author: JulianYGao
'''
from utils import *
import itertools

class TaxiMDP(object):
    '''
    classdocs
    '''
    def __init__(self, start_time, available_time, start_grid, grid_factor, rdd, 
        discount=1.0, boundaries=(-74.021611, -73.742833, 40.616669, 40.886116)):
        '''
        Constructor
        '''
        self.t_start = start_time
        self.t_end = get_state_time_stamp(start_time, available_time)[0]
        self.g_start = start_grid
        self.gamma = discount
        self.grid_scale = 0.00111 * grid_factor
        self.grids = rdd.map(lambda (k, v): k[0]).distinct().collect() #self._generate_grids(boundaries)
        self.hotspots = self._sort_hotspots(rdd)
        self.traffic_info = rdd.collectAsMap()
        self.boundaries = boundaries
        
    def isEnd(self, state):
        return get_state_time(state[1]) > self.t_end
        
    def startState(self):
        return (self.t_start, self.g_start)

    def actions(self, state):

        loc = ((float(state[0][0][0]), float(state[0][0][1])), 
            (float(state[0][1][0]), float(state[0][1][1])))
        lon, lat = loc[0], loc[1]

        # Stay in the old grid, driving around
        result = [self._stay(loc)]

        # Nine actions in all possible directions or stay
        if (lon[0] - self.grid_scale) > self.boundaries[0]:
            result.append(self._move_left(loc))
            if (lat[1] + self.grid_scale) < self.boundaries[3]:
                result.append(self._move_up_left(loc))

        if (lat[0] - self.grid_scale) > self.boundaries[2]:
            result.append(self._move_down(loc))
            if (lon[0] - self.grid_scale) > self.boundaries[0]:
                result.append(self._move_down_left(loc))

        if (lat[1] + self.grid_scale) < self.boundaries[3]:
            result.append(self._move_up(loc))
            if (lon[1] + self.grid_scale) < self.boundaries[1]:
                result.append(self._move_up_right(loc))
        
        if (lon[1] + self.grid_scale) < self.boundaries[1]:
            result.append(self._move_right(loc))
            if (lat[0] - self.grid_scale) > self.boundaries[2]:
                result.append(self._move_down_right(loc))

        # Don't consider all grids, but some of them... Only return sets of hot spots
        # Make top k as a parameter
        return result + self.hotspots[get_state_time_hr(state[1])]

    def prob_succ_reward(self, state, action):
        
        result = []
        curr_location = ((float(state[0][0][0]), float(state[0][0][1])), 
            (float(state[0][1][0]), float(state[0][1][1])))
        target_location_str = action
        current_time_hr = get_state_time_hr(state[1])
        
        # Have to make sure initial state is inside RDD
        # Current location and time is not necessarily in RDD. Return 0 in this case
        current_info = self.traffic_info.get((state[0], current_time_hr))
        if current_info:
            _, _, _, curr_cruise_time, v, curr_pickup_prob, dropoff_info = current_info
        else:
            # Doesn't really matter since pickup prob should be 0
            curr_cruise_time, v, curr_pickup_prob, dropoff_info = 2.0, 0.18, 0.0, None
            
        # Now it takes some time to drive to the target location
        target_location = ((float(target_location_str[0][0]), float(target_location_str[0][1])),
            (float(target_location_str[1][0]), float(target_location_str[1][1])))   
        travel_time = curr_cruise_time + manhattan_distance(target_location, curr_location) / v
        _, new_time_hr, new_time_str = get_state_time_stamp(state[1], travel_time)
        new_empty_state = (target_location_str, new_time_str)
        
        data = self.traffic_info.get((target_location_str, new_time_hr))
        if data:
            # Now if this state can be found in RDD
            # If not picking anyone currently
            distance_dist, time_dist, pay_dist, _, v, \
                target_pickup_prob, _ = data
            reward = self._state_reward(travel_time, distance_dist, time_dist, 
                pay_dist, target_pickup_prob)
            result.append((1 - curr_pickup_prob, new_empty_state, reward))
        else:
            # If is not in the database, then must have not been visited for a long time
            result.append((1 - curr_pickup_prob, new_empty_state, 0.0))

        # Needs to consider probability of picking up someone here.
        if dropoff_info:
            for dropoff_location in dropoff_info:
                dropoff_probability = dropoff_info[dropoff_location]
                trip_time = curr_cruise_time + manhattan_distance(((float(dropoff_location[0][0]), 
                    float(dropoff_location[0][1])), (float(dropoff_location[1][1]), 
                        float(dropoff_location[1][1]))), curr_location) / v
                _, dest_time_hr, dest_time_str = get_state_time_stamp(state[1], trip_time)
                new_full_state = (dropoff_location, dest_time_str)
                dest_info = self.traffic_info.get((dropoff_location, dest_time_hr))    
                if not dest_info:
                    new_reward = 0.0
                else:
                    dest_distance_dist, dest_time_dist, dest_pay_dist, _, _, dest_pickup_prob, _ = dest_info
                    new_reward = self._state_reward(trip_time, dest_distance_dist, 
                        dest_time_dist, dest_pay_dist, dest_pickup_prob)
                result.append((curr_pickup_prob * dropoff_probability, new_full_state, new_reward))
                
        return result

    def discount(self):
        return self.gamma
    
    def states(self):
        # First get all grids
        time, state = [], []
        for t_curr in time_range(get_state_time(self.t_start), self.t_end):
            time.append(t_curr.strftime('%H:%M'))
        # Then merge all grids and possible times
        for g in self.grids:
            state += zip([g] * len(time), time)
        return state
    
    def _state_reward(self, time, (dist_m, dist_std), (time_m, time_std), 
        (pay_m, pay_std), prob):
        return prob * ((pay_m - pay_std / 2.0) ** 2 / ((time_m - time_std / 2.0) * \
            (dist_m - dist_std / 2.0)))
        
    def _sort_hotspots(self, rdd):
        # Hot spots are the locations that have highest probabilities
        res = rdd.map(lambda ((grid, hr), ((d_m, d_v), (t_m, t_v), 
            (p_m, p_v), c_t, v, p, g)): (hr, (grid, p))).\
                filter(lambda x: x[1][1] > 0.5).sortBy(lambda x: x[1][1], 
                    ascending=False).map(lambda x: (x[0], [x[1][0]]))\
                        .reduceByKey(lambda a, b: a + b)
        hotspots = res.collectAsMap()
        for hr in hotspots:
            sorted_grid_lst = hotspots[hr]
            # Only consider the top 10 hot spots in the surroundings
            if len(sorted_grid_lst) < 10:
                continue
            hotspots[hr] = sorted_grid_lst[:5] + random.sample(sorted_grid_lst[5:], 5)
            # Use some randomness to add robustness
        return hotspots

    def _generate_grids(self, boundaries):
        # Choice 1: Generating the entire grid within the whole range. Not sure
        # if computationally feasible
        # Choice 2: Grabbing the grids which contains data
        lon0, lon1, lat0, lat1 = boundaries
        lon_lower, lat_lower = math.floor(lon0 / self.grid_scale ) * self.grid_scale, \
            math.floor(lat0 / self.grid_scale) * self.grid_scale
        lon_higher, lat_higher = math.ceil(lon1 / self.grid_scale ) * self.grid_scale, \
            math.ceil(lat1 / self.grid_scale) * self.grid_scale
        lon_range = np.arange(lon_lower, lon_higher + self.grid_scale, self.grid_scale)
        lat_range = np.arange(lat_lower, lat_higher + self.grid_scale, self.grid_scale)
        lon_tups, lat_tups = [], []
        for i in range(len(lon_range) - 1):
            lon_tups.append((str(lon_range[i]), str(lon_range[i + 1])))
        for j in range(len(lat_range) - 1):
            lat_tups.append((str(lat_range[j]), str(lat_range[j + 1])))
        return list(itertools.product(lon_tups, lat_tups))
       
    def _stay(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0]), str(loc[1][1])))

    def _move_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), 
            (str(loc[1][0]), str(loc[1][1])))

    def _move_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), 
             (str(loc[1][0]), str(loc[1][1])))

    def _move_up(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0] + self.grid_scale), 
             str(loc[1][1] + self.grid_scale)))

    def _move_down(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0] - self.grid_scale), 
            str(loc[1][1] - self.grid_scale)))

    def _move_up_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), \
            (str(loc[1][0] + self.grid_scale), str(loc[1][1] + self.grid_scale)))

    def _move_up_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), \
            (str(loc[1][0] + self.grid_scale), str(loc[1][1] + self.grid_scale)))

    def _move_down_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), \
            (str(loc[1][0] - self.grid_scale), str(loc[1][1] - self.grid_scale)))

    def _move_down_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), \
            (str(loc[1][0] - self.grid_scale), str(loc[1][1] - self.grid_scale)))



def valueIteration(mdp, f):
    
    V = defaultdict(float)
    states = mdp.states()

    def Q(state, action):
        return sum(prob * (reward + mdp.discount() * V[newState]) for prob, 
            newState, reward in mdp.prob_succ_reward(state, action))
    i = 0
    while True:
        print 'Iteration ' + str(i) + ' ============================================' 
        newV, policy = defaultdict(float), defaultdict()
        for s in states:
            if mdp.isEnd(s):
                newV[s] = 0.0
                policy[s] = None
            else:
                newV[s], policy[s] = max((Q(s, action), \
                    action) for action in mdp.actions(s))
        bestV, bestS = max((V[s], s) for s in states) 
        epsilon = max(abs(newV[s] - V[s]) for s in states)
        print 'Maximum utility: ' + str(bestV) + '\nState of maxQ: ' + str(bestS)\
             + '\nBest policy: ' + policy[bestS]
        print epsilon
        if epsilon < 1e-5:
            break  
        V = newV
        i += 1
    write_to_file(policy, V, f)
                
def profit_estimation(policy, initial_state, info, iters=80):
    """
    Note that initial state should match with the input start state
    for MDP value iteration as well
    """
    for _ in iters:

        distance_dist, time_dist, pay_dist, cruise_time, v, target_pickup_prob, \
            dropoff_prob = info.lookup(initial_state)
        state = initial_state
        while True:
            next_loc = policy[state](state)
            time_dist = 3
        
    
    
    