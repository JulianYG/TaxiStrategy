import csv
from collections import defaultdict as ddict
import gmaps
import plotly.plotly as py
import plotly.graph_objs as go
 
gmaps.configure(api_key="AIzaSyCB9rVMgEbTcYaQhnbM6jBzLjFLXLaGJZ8")
 
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
            point = ((lat0 + lat1) / 2, (lon0 + lon1) / 2, weight)
            timeMap[time].append(point)
     
    overlapped_map = gmaps.Map()
    for i in range(24):
        data = timeMap[i]
        heatmap_layer = gmaps.WeightedHeatmap(data=data)
        heatmap_layer.max_intensity = 1
        maps[i].add_layer(heatmap_layer)
        overlapped_map.add_layer(heatmap_layer)
 
    overlapped_map


