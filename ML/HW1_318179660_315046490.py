import os
import sys
import argparse
import time
import itertools
import numpy as np
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier


class KnnClassifier:
    def __init__(self, k: int, p: float):
        """
        Constructor for the KnnClassifier.

        :param k: Number of nearest neighbors to use.
        :param p: p parameter for Minkowski distance calculation.
        """
        self.k = k
        self.p = p
        self.train_set = []
        self.train_label = []
        self.ids = (315046490, 318179660)

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        """
        This method trains a k-NN classifier on a given training set X with label set y.

        :param X: A 2-dimensional numpy array of m rows and d columns. It is guaranteed that m >= 1 and d >= 1.
            Array datatype is guaranteed to be np.float32.
        :param y: A 1-dimensional numpy array of m rows. it is guaranteed to match X's rows in length (|m_x| == |m_y|).
            Array datatype is guaranteed to be np.uint8.
        """
        self.train_set = X
        self.train_label = y

    def _tie_break(self, winning_labels, distances, labels):
        indecies = np.isin(labels, winning_labels)
        distances = distances[indecies]
        min_distance = min(distances)
        closest_winners = np.where(distances == min_distance)
        closest_winners_label = labels[closest_winners]
        closest_winners_label_sorted = sorted(closest_winners_label)
        return closest_winners_label_sorted[0]

    def _single_predict(self, sample):
        """
        Predicts the label of a single sample
        :param sample: sample to predict it's label, ndarray type.
        :return: integer representing the label.
        """
        # Creating a lambda function to calculated distance between two given vectors
        single_dist = lambda x: sum((abs(x[0] - x[1])) ** self.p) ** (1. / self.p)
        # Create a local copy of the training set
        train_set = np.array(self.train_set)
        # Create a matrix where every row is the given sample point
        helper_mat = np.ones(train_set.shape) * sample
        # Calculate the distance for each point in the training set using single_dist
        dist = np.fromiter(map(single_dist, zip(helper_mat, train_set)), dtype=np.float64)
        dist_and_label = np.array(
            sorted(sorted(list(zip(dist, self.train_label)), key=lambda x: x[1]), key=lambda x: x[0])[
            :self.k])
        from collections import Counter
        distances, labels = list(zip(*dist_and_label))
        counter = dict(Counter(labels)).items()
        distances = np.array(distances)
        labels = np.array(labels)
        max_label_appearance = max(counter, key=lambda x: x[1])[1]
        counter = np.array(list(counter))
        counter = counter[np.where(counter[:, 1] == max_label_appearance)]
        if len(counter) >= 2:
            return self._tie_break(counter.T[0], distances, labels)
        chosen_label = counter[0][0]
        return chosen_label

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        This method predicts the y labels of a given dataset X, based on a previous training of the model.
        It is mandatory to call KnnClassifier.fit before calling this method.

        :param X: A 2-dimensional numpy array of m rows and d columns. It is guaranteed that m >= 1 and d >= 1.
            Array datatype is guaranteed to be np.float32.
        :return: A 1-dimensional numpy array of m rows. Should be of datatype np.uint8.
        """

        predictions = np.array(list(map(self._single_predict, X)))
        return predictions


def main():
    print("*" * 20)
    print("Started HW1_ID1_ID2.py")
    # Parsing script arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', type=str, help='Input csv file path')
    parser.add_argument('k', type=int, help='k parameter')
    parser.add_argument('p', type=float, help='p parameter')
    args = parser.parse_args()

    print("Processed input arguments:")
    print(f"csv = {args.csv}, k = {args.k}, p = {args.p}")
    np.random.seed(42)
    counter = 0
    for i in range(40):
        model = KnnClassifier(k=args.k, p=args.p)
        data = pd.read_csv(args.csv, header=None)
        X = data[data.columns[:-1]].values.astype(np.float32)
        y = pd.factorize(data[data.columns[-1]])[0].astype(np.uint8)
        indecies = np.arange(150)
        np.random.shuffle(indecies)
        indecies_train = indecies[:100]
        indecies_test = indecies[100:150]
        X1 = X[indecies_train]
        y1 = y[indecies_train]
        X2 = X[indecies_test]
        y2 = y[indecies_test]
        model.fit(X1, y1)
        y_pred = model.predict(X2)
        accuracy = np.sum(y_pred == y2) / len(y2)
        print(f"Our Train accuracy: {accuracy * 100 :.2f}%")
        classifier = KNeighborsClassifier(4, p=2)
        classifier.fit(X1, y1)
        y_pred = classifier.predict(X2)
        accuracy1 = np.sum(y_pred == y2) / len(y2)
        print(f"Sklearn Train accuracy: {accuracy1 * 100 :.2f}%")
        print("*" * 20)
        if accuracy1 != accuracy:
            counter += 1
    print("Number of missmatched accuracies:", counter)


if __name__ == "__main__":
    main()
