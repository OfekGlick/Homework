import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import KFold


def cross_validation_error(X, y, model, folds):
    n_sample = len(y)
    indecies = np.arange(n_sample)
    np.random.shuffle(indecies)
    fold_sizes = np.full(folds, n_sample // folds, dtype=int)
    fold_sizes[:n_sample % folds] += 1
    current = 0
    train_err_lst = []
    test_err_lst = []
    for fold_size in fold_sizes:
        start, stop = current, current + fold_size
        test_indecies = indecies[start:stop]
        test_mask = np.zeros(n_sample)
        test_mask[test_indecies] = True
        train_indecies = indecies[np.logical_not(test_mask)]

        X_train = X[train_indecies]
        y_train = y[train_indecies]
        X_test = X[test_indecies]
        y_test = y[test_indecies]

        model.fit(X_train, y_train)
        pred_train = model.predict(X_train)
        pred_test = model.predict(X_test)
        train_err = 1 - (np.sum(pred_train == y_train) / len(y_train))
        test_err = 1 - (np.sum(pred_test == y_test) / len(y_test))

        train_err_lst.append(train_err)
        test_err_lst.append(test_err)

        current = stop
    avg_train_err = sum(train_err_lst) / len(train_err_lst)
    avg_test_err = sum(test_err_lst) / len(test_err_lst)
    return avg_train_err, avg_test_err


def logistical_regression_results(X_train, y_train, X_test, y_test):
    C = [1 / 1e-4, 1 / 1e-2, 1, 1 / 1e2, 1 / 1e4]
    res = dict()
    from sklearn.linear_model import LogisticRegression
    for c in C:
        model = LogisticRegression(C=c, max_iter=10000)
        avg_train_err, avg_val_err = cross_validation_error(X_train, y_train, model, 5)
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        test_err = 1 - (np.sum(pred == y_test) / len(y_test))
        res['logistic_regression_lambda_' + str(1 / c)] = (avg_train_err, avg_val_err, test_err)
    return res


if __name__ == '__main__':
    from sklearn.datasets import load_iris

    iris_data = load_iris()
    X, y = iris_data['data'], iris_data['target']
    from sklearn.model_selection import train_test_split

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=7)
    res = logistical_regression_results(X_train, y_train, X_test, y_test)
    lambdas = [0.0001, 0.01, 1, 100, 10000]
    x = np.arange(len(lambdas))
    width = 0.20
    average_train_error = []
    average_val_error = []
    test_error = []
    for i in res.values():
        average_train_error.append(i[0])
        average_val_error.append(i[1])
        test_error.append(i[2])
    fig, ax = plt.subplots()
    ax.bar(x + width, average_train_error, width, label="Average Train Error")
    ax.bar(x, average_val_error, width, label="Average Validation Error")
    ax.bar(x - width, test_error, width, label="Test Error")
    ax.set_ylabel("Error Rate")
    ax.set_xlabel("Lambda")
    ax.set_title("Error rate over different lambda values")
    ax.set_xticks(x)
    ax.set_xticklabels(lambdas)
    ax.legend()
    fig.tight_layout()
    plt.show()
