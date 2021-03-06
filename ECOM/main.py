from typing import List, Set, Dict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import networkx
import copy


def calc_concerned_level(graph, infected, susceptible):
    susceptible_nodes = {
        node: sum([1 for node_1 in graph.neighbors(node) if node_1 in infected]) / graph.degree(node) for node in
        susceptible if graph.degree(node) != 0}
    infected_nodes = {node: 0 for node in infected}
    return susceptible_nodes, infected_nodes


def LTM(graph: networkx.Graph, patients_0: List, iterations: int) -> Set:
    total_infected = set(patients_0)
    susceptible = graph.nodes - total_infected
    susceptible_nodes = {node: 0 for node in susceptible}
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
        susceptible_nodes, infected_nodes = calc_concerned_level(graph, total_infected, susceptible)
        networkx.set_node_attributes(graph, susceptible_nodes, "concerned")
        networkx.set_node_attributes(graph, infected_nodes, "concerned")
        total_infected = temp_infected
        susceptible = temp_susceptible
        iterations -= 1
    return total_infected


def calc_concerned_level_ICM(graph, susceptible, infected, removed):
    susceptible_nodes = {
        node: min((sum([1 for node_1 in graph.neighbors(node) if node_1 in infected]) +
                   (3 * sum([1 for node_1 in graph.neighbors(node) if node_1 in removed])))
                  / graph.degree(node), 1) for node in susceptible if graph.degree(node) != 0}
    return susceptible_nodes


def ICM(graph: networkx.Graph, patients_0: List, iterations: int) -> [Set, Set]:
    total_infected = set(patients_0)
    susceptible = set(graph.nodes) - total_infected
    total_deceased = set()
    susceptible_nodes = {node: 0 for node in susceptible}
    networkx.set_node_attributes(graph, susceptible_nodes, "concerned")
    for node in total_infected:
        prob = np.random.random()
        if prob <= LETHALITY:
            total_deceased.add(node)
    total_infected = total_infected - total_deceased
    NI = total_infected.copy()
    while iterations > 0:
        temp_susceptible = susceptible.copy()
        NI_temp = set()
        temp_deceased = set()
        for node in NI:
            sus_neighbors = {node_2 for node_2 in graph.neighbors(node) if node_2 in susceptible}
            for neighbor in sus_neighbors:
                inf_prob = np.random.random()
                if inf_prob <= min(1, CONTAGION * graph.edges[(node, neighbor)]['w'] * (
                        1 - graph.nodes[neighbor]["concerned"])):
                    death_prob = np.random.random()
                    if neighbor in temp_susceptible:
                        if death_prob <= LETHALITY:
                            temp_deceased.add(neighbor)
                        else:
                            NI_temp.add(neighbor)
                        try:
                            temp_susceptible.remove(neighbor)
                        except KeyError:
                            continue

        susceptible = temp_susceptible.copy()
        susceptible_nodes = calc_concerned_level_ICM(graph, susceptible, total_infected, total_deceased)
        networkx.set_node_attributes(graph, susceptible_nodes, "concerned")
        total_infected = set.union(total_infected, NI_temp)
        total_deceased = set.union(total_deceased, temp_deceased)
        NI = NI_temp.copy()
        iterations -= 1
    return total_infected, total_deceased


def plot_degree_histogram(histogram: Dict):
    plt.bar(histogram.keys(), histogram.values(), 2, align='center')
    plt.show()


def calc_degree_histogram(graph: networkx.Graph) -> Dict:
    histogram = {}
    deg_list = list(graph.degree())
    for node, deg in deg_list:
        if deg in histogram.keys():
            histogram[deg] += 1
        else:
            histogram[deg] = 1
    return histogram


def build_graph(filename: str) -> networkx.Graph:
    df = pd.read_csv(filename)
    _, col = df.shape
    if col == 2:
        df['w'] = 1
    source, target, att = df.columns
    G = networkx.from_pandas_edgelist(df, source, target, att)
    return G


def clustering_coefficient(graph: networkx.Graph) -> float:
    adj_matrix = networkx.convert_matrix.to_numpy_matrix(graph)
    adj_matrix[adj_matrix > 0] = 1
    two_edges_trip = adj_matrix @ adj_matrix
    three_edges_trip = two_edges_trip @ adj_matrix
    connect_trip = np.sum(two_edges_trip) - np.sum(np.trace(two_edges_trip))
    triangles = np.sum(np.trace(three_edges_trip))
    return triangles / connect_trip


def compute_lethality_effect(graph: networkx.Graph, t: int) -> [Dict, Dict]:
    global LETHALITY
    mean_deaths = {}
    mean_infected = {}
    for l in (.05, .15, .3, .5, .7):
        LETHALITY = l
        temp1 = []
        temp2 = []
        for iteration in range(30):
            print(iteration)
            G = copy.deepcopy(graph)
            patients_0 = np.random.choice(list(G.nodes), size=50, replace=False, p=None)
            infected, death = ICM(graph, patients_0, t)
            temp1.append(len(infected))
            temp2.append(len(death))
        from statistics import mean
        mean_infected[l], mean_deaths[l] = mean(temp1), mean(temp2)
    return mean_deaths, mean_infected


def plot_lethality_effect(mean_deaths: Dict, mean_infected: Dict):
    plt.plot(list(mean_deaths.keys()), list(mean_deaths.values()))
    plt.plot(list(mean_infected.keys()), list(mean_infected.values()))
    plt.show()


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
CONTAGION = 0.8
LETHALITY = .2

if __name__ == "__main__":
    filename = ["PartA1.csv", "PartA2.csv", "PartB-C.csv"]
    patients = "patients0.csv"
    G = build_graph(filename=filename[2])
    patients0 = set(pd.read_csv('patients0.csv', header=None)[:50][0].values.tolist())
    fi = []
    fr = []
    for j in range(30):
        i, r = ICM(G, patients0, 6)
        fi.append(len(i))
        fr.append(len(r))
        print(j, ":\n", len(i), i, "\n", len(r), r)
    print(np.average(np.array(fi)))
    print(np.average(np.array(fr)))
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
