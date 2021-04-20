import os
import sys
import argparse
import time
import itertools
import numpy as np
import pandas as pd


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
        # TODO - Place your student IDs here. Single submitters please use a tuple like so: self.ids = (123456789,)
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

    def _single_predict(self, sample):
        single_dist = lambda x: sum((x[0] - x[1]) ** self.p) ** (1 / self.p)
        train_set = np.array(self.train_set)
        helper_mat = np.ones(train_set.shape) * sample
        dist = np.fromiter(map(single_dist, zip(helper_mat, train_set)), dtype=np.float64)
        dist_and_label = sorted(sorted(list(zip(dist, self.train_label)), key=lambda x: x[1]), key=lambda x: x[0])[
                         :self.k]
        from collections import Counter
        counter = dict(Counter(list(zip(*dist_and_label))[1])).items()
        max_label_appearance = max(counter, key=lambda x: x[1])[1]
        counter = np.array(list(counter))
        counter = counter[np.where(counter[:, 1] == max_label_appearance)].tolist()
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
        ofek = 5
        return predictions

        ### Example code - don't use this:
        # return np.random.randint(low=0, high=2, size=len(X), dtype=np.uint8)


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

    print("Initiating KnnClassifier")
    model = KnnClassifier(k=args.k, p=args.p)
    print(f"Student IDs: {model.ids}")
    print(f"Loading data from {args.csv}...")
    data = pd.read_csv(args.csv, header=None)
    print(f"Loaded {data.shape[0]} rows and {data.shape[1]} columns")
    X = data[data.columns[:-1]].values.astype(np.float32)
    y = pd.factorize(data[data.columns[-1]])[0].astype(np.uint8)

    print("Fitting...")
    model.fit(X, y)
    print("Done")
    print("Predicting...")
    y_pred = model.predict(X)
    print("Done")
    accuracy = np.sum(y_pred == y) / len(y)
    print(f"Train accuracy: {accuracy * 100 :.2f}%")
    print("*" * 20)


if __name__ == "__main__":
    main()
