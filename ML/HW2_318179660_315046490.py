import os
import sys
import argparse
import time
import itertools
import numpy as np
import pandas as pd


class PerceptronClassifier:
    def __init__(self):
        """
        Constructor for the PerceptronClassifier.
        """

        self.train_set = []
        self.train_label = []
        self.k = []
        self.ids = (318179660, 315046490)
        self.w = []

    def fit(self, X: np.ndarray, y: np.ndarray):
        """
        This method trains a multiclass perceptron classifier on a given training set X with label set y.
        :param X: A 2-dimensional numpy array of m rows and d columns. It is guaranteed that m >= 1 and d >= 1.
        Array datatype is guaranteed to be np.float32.
        :param y: A 1-dimensional numpy array of m rows. it is guaranteed to match X's rows in length (|m_x| == |m_y|).
        Array datatype is guaranteed to be np.uint8.
        """
        m, n = X.shape
        bias = np.ones((m, n + 1))
        bias[0:, 1:] = X
        self.train_set = bias
        self.train_label = y
        self.k = len(self.train_label[np.unique(self.train_label)])
        seps = np.zeros((self.k, n + 1))
        while True:
            prev = seps.copy()
            for x, y_t in zip(self.train_set, self.train_label):
                temp = np.array([w_t.dot(x) for w_t in seps])
                y_t_hat = np.argmax(temp)
                if y_t_hat != y_t:
                    seps[y_t] += x
                    seps[y_t_hat] -= x
            if np.array_equal(prev, seps):
                break
        self.w = seps
        return seps

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        This method predicts the y labels of a given dataset X, based on a previous training of the model.
        It is mandatory to call PerceptronClassifier.fit before calling this method.
        :param X: A 2-dimensional numpy array of m rows and d columns. It is guaranteed that m >= 1 and d >= 1.
        Array datatype is guaranteed to be np.float32.
        :return: A 1-dimensional numpy array of m rows. Should be of datatype np.uint8.
        """
        m, n = X.shape
        test_set = np.ones((m, n + 1))
        test_set[0:, 1:] = X
        return np.array([np.argmax(np.array([w_i.dot(x) for w_i in self.w])) for x in test_set])


if __name__ == "__main__":
    print("*" * 20)
    print("Started HW2_ID1_ID2.py")
    # Parsing script arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', type=str, help='Input csv file path')
    args = parser.parse_args()

    print("Processed input arguments:")
    print(f"csv = {args.csv}")

    print("Initiating PerceptronClassifier")
    model = PerceptronClassifier()
    print(f"Student IDs: {model.ids}")
    print(f"Loading data from {args.csv}...")
    data = pd.read_csv(args.csv, header=None)
    print(f"Loaded {data.shape[0]} rows and {data.shape[1]} columns")
    X = data[data.columns[:-1]].values.astype(np.float32)
    y = pd.factorize(data[data.columns[-1]])[0].astype(np.uint8)
    np.random.seed(42)
    acc_list = []
    for i in range(10, 150, 10):
        indecies = np.arange(150)
        np.random.shuffle(indecies)
        indecies_train = indecies[:i]
        indecies_test = indecies[i:150]
        X1 = X[indecies_train]
        y1 = y[indecies_train]
        X2 = X[indecies_test]
        y2 = y[indecies_test]
        is_separable = model.fit(X1, y1)
        y_pred = model.predict(X2)
        accuracy = np.sum(y_pred == y2.ravel()) / y.shape[0]
        acc_list.append(accuracy)
    print(acc_list)
