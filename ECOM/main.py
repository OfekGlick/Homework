from typing import List, Set, Dict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import networkx
import copy


def LTM(graph: networkx.Graph, patients_0: List, iterations: int) -> Set:
    total_infected = set(patients_0)
    # TODO implement your code here
    return total_infected


def ICM(graph: networkx.Graph, patients_0: List, iterations: int) -> [Set, Set]:
    total_infected = set(patients_0)
    total_deceased = set()
    # TODO implement your code here
    return total_infected, total_deceased


def plot_degree_histogram(histogram: Dict):
    plt.bar(range(len(histogram)),list(histogram.values()),align='center')
    plt.xticks(range(len(histogram)),list(histogram.values()))
    plt.show()


def calc_degree_histogram(graph: networkx.Graph) -> Dict:
    """
    Example:
    if histogram[1] = 10 -> 10 nodes have only 1 friend
    """
    histogram = {}
    deg_list = list(graph.degree())
    for node, deg in deg_list:
        if deg in histogram.keys():
            histogram[deg] += 1
        else:
            histogram[deg] = 1
    return histogram


def build_graph(filename: str) -> networkx.Graph:
    G = networkx.Graph()
    df = pd.read_csv(filename)
    _, col = df.shape
    if col == 2:
        df['weight'] = 1
    source, target, att = df.columns
    G = networkx.from_pandas_edgelist(df, source, target, att)
    return G


def clustering_coefficient(graph: networkx.Graph) -> float:
    triangle = sum(networkx.triangles(graph).values())
    return cc


def compute_lethality_effect(graph: networkx.Graph, t: int) -> [Dict, Dict]:
    global LETHALITY
    mean_deaths = {}
    mean_infected = {}
    for l in (.05, .15, .3, .5, .7):
        LETHALITY = l
        for iteration in range(30):
            G = copy.deepcopy(graph)
            patients_0 = np.random.choice(list(G.nodes), size=50, replace=False, p=None)
            # TODO implement your code here

    return mean_deaths, mean_infected


def plot_lethality_effect(mean_deaths: Dict, mean_infected: Dict):
    # TODO implement your code here
    ...


def choose_who_to_vaccinate(graph: networkx.Graph) -> List:
    people_to_vaccinate = []
    # TODO implement your code here
    return people_to_vaccinate


def choose_who_to_vaccinate_example(graph: networkx.Graph) -> List:
    """
    The following heuristic for Part C is simply taking the top 50 friendly people;
     that is, it returns the top 50 nodes in the graph with the highest degree.
    """
    node2degree = dict(graph.degree)
    sorted_nodes = sorted(node2degree.items(), key=lambda item: item[1], reverse=True)[:50]
    people_to_vaccinate = [node[0] for node in sorted_nodes]
    return people_to_vaccinate


"Global Hyper-parameters"
CONTAGION = 1
LETHALITY = .15

if __name__ == "__main__":
    filename = "PartA1.csv"
    G = build_graph(filename=filename)
    hist = calc_degree_histogram(G)
    plot_degree_histogram(hist)
    filename = "PartA2.csv"
    G = build_graph(filename=filename)
    hist = calc_degree_histogram(G)
    plot_degree_histogram(hist)
    filename = "PartB-C.csv"
    G = build_graph(filename=filename)
    hist = calc_degree_histogram(G)
    plot_degree_histogram(hist)