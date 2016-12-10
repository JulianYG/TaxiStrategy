import csv
from collections import defaultdict as ddict
import gmaps
import plotly.plotly as py
import plotly.graph_objs as go
from utils import *
import random
import sys
# from pyspark import SparkContext
 
gmaps.configure(api_key="AIzaSyCB9rVMgEbTcYaQhnbM6jBzLjFLXLaGJZ8")
csv.field_size_limit(sys.maxsize)
# sc = SparkContext(appName='plot')

def plot_histogram(b):
    data = go.Histogram(x=b)
    layout = go.Layout(yaxis=dict(type='log', autorange=True))
    fig = go.Figure(data=[data], layout=layout)
    py.iplot(fig)
     
def plot_probability_map():
    timeMap = ddict(list)
    maps = []
    for i in range(24):
        maps.append(gmaps.Map())
     
    with open('params/probmap', 'r') as data:
        reader = csv.reader(data, delimiter=',')
        for row in reader:
            lat0 = float(row[2].replace("'", ""))
            lat1 = float(row[3].replace("'", ""))
            lon0 = float(row[0].replace("'", ""))
            lon1 = float(row[1].replace("'", ""))
            weight = float(row[-1].replace("'", ""))
            time = int(row[4].replace("'", ""))
            dot = centerize_grid(((lon0, lon1), (lat0, lat1)))
            point = (dot[1], dot[0], weight)
            timeMap[time].append(point)
     
    overlapped_map = gmaps.Map()
    for i in range(24):
        data = timeMap[i]
        heatmap_layer = gmaps.WeightedHeatmap(data=data)
        heatmap_layer.max_intensity = 1
        maps[i].add_layer(heatmap_layer)
        overlapped_map.add_layer(heatmap_layer)
 
    overlapped_map

def plot_dropoff_distribution():
    timeLocMap = ddict(list)
    # d = sc.textFile('dropmap')
    with open('params/dropmap', 'r') as data:
        reader = csv.reader(data, delimiter=',')
        for row in reader:
            dot = centerize_grid(((float(row[0]), float(row[1])), 
                (float(row[2]), float(row[3]))))
            locs = row[5].split(' ')
            for loc in locs:
                coords = loc.split(':')
                timeLocMap[(int(row[4]), dot)].append((float(coords[1]), float(coords[0])))
                # lat, lon

    gmap = gmaps.Map()
    start = random.choice(timeLocMap.keys())
    print start
    print timeLocMap[start]
    pickup_layer = gmaps.symbol_layer([(start[1][1], start[1][0])], fill_color='green', 
        stroke_color='green', scale=2)
    dropoff_layer = gmaps.Heatmap(data=timeLocMap[start])
    gmap.add_layer(pickup_layer)
    gmap.add_layer(dropoff_layer)
    gmap

plot_dropoff_distribution()
