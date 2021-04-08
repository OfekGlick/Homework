from typing import List, Set, Dict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import networkx
import copy


def LTM(graph: networkx.Graph, patients_0: List, iterations: int) -> Set:
    total_infected = set(patients_0)
    susceptible = graph.nodes - total_infected
    susceptible_nodes = {
        node: sum([1 for node_1 in graph.neighbors(node) if node_1 in total_infected]) / graph.degree(node) for node in
        susceptible}
    infected_nodes = {node: 0 for node in total_infected}
    networkx.set_node_attributes(graph, susceptible_nodes, "concerned")
    networkx.set_node_attributes(graph, infected_nodes, "concerned")
    while iterations > 0:
        temp_infected = total_infected.copy()
        temp_susceptible = susceptible.copy()
        for node in susceptible:
            sum_of_weights = CONTAGION * sum(
                [graph.edges[(node_1, node_2)]["w"] for (node_1, node_2) in networkx.edges(graph, node) if
                 node_2 in total_infected])
            threshhold = 1 + graph.nodes[node]["concerned"]
            if sum_of_weights >= threshhold:
                temp_susceptible.remove(node)
                temp_infected.add(node)
        total_infected = temp_infected
        susceptible = temp_susceptible
        iterations -= 1
    return total_infected


def ICM(graph: networkx.Graph, patients_0: List, iterations: int) -> [Set, Set]:
    total_infected = set(patients_0)
    susceptible = graph.nodes - total_infected
    total_deceased = {}
    # TODO implement your code here
    return total_infected, total_deceased


def plot_degree_histogram(histogram: Dict):
    plt.bar(range(len(histogram)), list(histogram.values()), align='center')
    plt.xticks(range(len(histogram)), list(histogram.values()))
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
        df['w'] = 1
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
CONTAGION = 2
LETHALITY = .15

if __name__ == "__main__":
    filename = "PartB-C.csv"
    G = build_graph(filename=filename)
    inf = [0]
    bla = LTM(G, inf, 1)
    bla2 = LTM(G, inf, 2)
    bla3 = LTM(G, inf, 3)
    bla4 = LTM(G, inf, 6)

    print(len(bla))
    print(len(bla2))
    print(len(bla3))
    print(len(bla4))

    # hist = calc_degree_histogram(G)
    # plot_degree_histogram(hist)
    # filename = "PartA2.csv"
    # G = build_graph(filename=filename)
    # hist = calc_degree_histogram(G)
    # plot_degree_histogram(hist)
    # filename = "PartB-C.csv"
    # G = build_graph(filename=filename)
    # hist = calc_degree_histogram(G)
    # plot_degree_histogram(hist)
