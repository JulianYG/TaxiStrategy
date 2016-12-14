'''
Created on Dec 11, 2016

@author: JulianYGao
'''
from utils import *

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

        # If you narrow down the map range by too much, there may be no 
        # places the driver can go. (When all hotspots are out of boundaries, 
        # and neighbors unknown, it will just randomly wander to infertile neighbors).
        # keep that in mind!
        self.boundaries = boundaries
        self.grids = self._generate_grids(rdd)
        self.hotspots = sort_hotspots(rdd)
        self.traffic_info = rdd.collectAsMap()
        
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
        target_location = action
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
        # This part can be improved by Brehensam's algorithm, if 
        # want to be more accurate on travel time
        travel_time = curr_cruise_time + manhattan_distance(target_location, curr_location) / v
        _, new_time_hr, new_time_str = get_state_time_stamp(state[1], travel_time)
        new_empty_state = (target_location, new_time_str)
        
        data = self.traffic_info.get((target_location, new_time_hr))
        if data:
            # Now if this state can be found in RDD
            # If not picking anyone currently
            distance_dist, time_dist, pay_dist, _, v, \
                target_pickup_prob, _ = data
            reward = get_state_reward(travel_time, distance_dist, time_dist, 
                pay_dist, target_pickup_prob)
            result.append((1 - curr_pickup_prob, new_empty_state, reward))
        else:
            # If is not in the database, then must have not been visited for a long time
            result.append((1 - curr_pickup_prob, new_empty_state, 0.0))

        # Needs to consider probability of picking up someone here.
        if dropoff_info:
            for dropoff_location in dropoff_info:
                dropoff_probability = dropoff_info[dropoff_location]
                trip_time = curr_cruise_time + manhattan_distance(dropoff_location, curr_location) / v
                _, dest_time_hr, dest_time_str = get_state_time_stamp(state[1], trip_time)
                new_full_state = (dropoff_location, dest_time_str)
                dest_info = self.traffic_info.get((dropoff_location, dest_time_hr))    
                if not dest_info:
                    new_reward = 0.0
                else:
                    dest_distance_dist, dest_time_dist, dest_pay_dist, _, _, dest_pickup_prob, _ = dest_info
                    new_reward = get_state_reward(trip_time, dest_distance_dist, 
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

    def _generate_grids(self, rdd, onfly=1):
        # Choice 1: Generating the entire grid within the whole range. Not sure
        # if computationally feasible
        # Choice 2: Grabbing the grids which contains data
        # I use choice 2: Instead of generating wasted states beforehand, use 
        # the same evaluation method on-fly during planning phase
        if onfly:
            # Only return grids within boundaries
            return rdd.map(lambda (k, v): k[0]).filter(self._boundary_check)\
                .distinct().collect()
        else:
            lon0, lon1, lat0, lat1 = self.boundaries
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
    
    def _boundary_check(self, x):
        return float(x[0][0]) > self.boundaries[0] and float(x[0][1]) < \
            self.boundaries[1] and float(x[1][0]) > self.boundaries[2] and \
                float(x[1][1]) < self.boundaries[3]  

# The MDP value iteration process
def valueIteration(mdp, f):
    
    V = defaultdict(float)
    states = mdp.states()
    iter_log = open(f + '_log', 'w')

    def Q(state, action):
        return sum(prob * (reward + mdp.discount() * V[newState]) for prob, 
            newState, reward in mdp.prob_succ_reward(state, action))
    i = 0
    while True:
        title = 'Iteration ' + str(i) + '\n'
        print title
        iter_log.write(title) 
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
        iter_log.write('Maximum utility: ' + str(bestV) + '\nState of maxQ: ' + str(bestS)\
             + '\nBest policy: ' + str(policy[bestS]) + '\n')
        error_margin = str(epsilon) + '\n'
        print error_margin
        iter_log.write(error_margin)
        if epsilon < 1e-5:
            iter_log.close()
            break
        V = newV
        i += 1
    write_to_file(policy, V, f)
    
    
    