import numpy as np
import matplotlib.pyplot as plt


def action1(A: np.ndarray, x: np.ndarray, n):
    """
    Helper method for ex1, does the calculation of B
    :param A: Matrix from ex1
    :param x: vector from ex1
    :param n: Dimensions of elements
    :return: B as requested in the exercise
    """
    A = A.transpose()
    temp = np.zeros(A.shape) + x
    temp = temp.transpose()
    scalars = np.array(range(1, n + 1)).reshape((n, 1))
    B = A + temp * scalars[:np.newaxis]
    np.fill_diagonal(B, 0)
    return B


def ex1(A: np.ndarray, x: np.ndarray):
    """
    Converts A and x into B according to the specifications in the exercise.
    :param A: Square ndarray (n,n)
    :param x: vector ndarray with same dimensions as A (n,1)
    :return: B - square ndarray if input is valid, None otherwise.
    """
    if isinstance(A, np.ndarray) and isinstance(x, np.ndarray):
        (m1, n1) = A.shape
        (m2, n2) = x.shape
        if m1 == n1 and m2 == m1 and n2 == 1:
            return action1(A, x, m1)
    else:
        raise TypeError("Invalid input, A needs to be square ndarray and x needs to conform in dimensionality")


def action2(A: np.ndarray, B: np.ndarray, n: int, b: np.ndarray):
    """
    Helper method for ex2, does the calculation of Qz
    :param A: Matrix from ex2
    :param B: Matrix from ex2
    :param n: Integer
    :param b: Vector from ex2
    :return: Qz as ndarray as requested in the exercise
    """
    y = np.kron(np.array(range(1, n + 1)).reshape((n, 1)), b)
    z = np.kron(np.ones((b.shape)), y)
    P = np.kron(np.eye(n), A) + np.kron(np.eye(n, k=1), B.transpose()) + np.kron(np.eye(n, k=-1), B)
    Q = np.kron(A, P)
    return np.linalg.solve(Q, z)


def ex2(A: np.ndarray, B: np.ndarray, n: int, b: np.ndarray):
    """
    Calculates Qz as specified in the exercise
    :param A: Square ndarray of dimensions (m,m)
    :param B: Square ndarray of dimensions (m,m)
    :param n: Integer larger than 3
    :param b: ndarray vector of dimensions (m,1)
    :return: Qz as specified in the exercise
    """
    if isinstance(A, np.ndarray) and isinstance(B, np.ndarray) and isinstance(b, np.ndarray) and isinstance(n, int):
        (m1, n1) = A.shape
        (m2, n2) = B.shape
        (m3, n3) = b.shape
        if m1 == n1 and m2 == m1 and n2 == m2 and m3 == m1 and n3 == 1 and n >= 4:
            return action2(A, B, n, b)
    else:
        raise TypeError("Invalid input")


def f(X, param):
    return (param[0] + param[1] * X + param[2] * X ** 2) / (param[3] + param[4] * X + param[5] * X ** 2)


def plot_graph(X: np.ndarray, coefficients, name: str):
    xlist = np.linspace(-1.5, 1.5, num=1000)
    ylist = f(xlist, param=coefficients)
    plt.plot(xlist, ylist)
    plt.scatter(X[0], X[1])
    plt.savefig('fig_' + name + ".png")
    plt.clf()


def fit_rational(X: np.ndarray):
    (m, n) = X.shape
    b = -1 * X[1]
    b = b.reshape(n, 1)
    A = np.concatenate((-1 * np.ones((n, 1)), (-1 * X[0]).reshape(n, 1), (-1 * X[0] * X[0]).reshape(n, 1),
                        (X[0] * X[1]).reshape(n, 1), (X[0] * X[0] * X[1]).reshape(n, 1)), axis=1)
    sol = np.linalg.lstsq(A, b, rcond=None)[0].reshape((1, 5))[0]
    sol = np.insert(sol, 3, 1)
    plot_graph(X, sol, 'not_normed')
    return sol


def fit_rational_normed(X: np.ndarray):
    (m, n) = X.shape
    A = np.concatenate(
        (-1 * np.ones((n, 1)), (-1 * X[0]).reshape(n, 1), (-1 * X[0] * X[0]).reshape(n, 1), X[1].reshape((n, 1)),
         (X[0] * X[1]).reshape(n, 1), (X[0] * X[0] * X[1]).reshape(n, 1)), axis=1)
    sol = np.linalg.eig(A.T @ A)[1][:, -1]
    plot_graph(X, sol, 'normed')
    return sol


def main():
    # Question 1
    A = np.array([[1, -2, 3, 7], [4, 5, 6, 7], [-7, 8, 9, 7], [10, -11, 12, 7]])
    x = np.array([[17], [6], [-3], [0]])
    print("Ex1:")
    print(ex1(A, x))
    A = np.array(range(1, 10)).reshape((3, 3))
    B = A + np.ones((3, 3)) * 2
    b = np.array(range(1, 4)).reshape((3, 1))
    n = 4
    print("Ex2:")
    print(ex2(A, B, n, b))
    X = np.array([[-0.966175231649752, -0.920529100440521, -0.871040946427231, -0.792416754493313, -0.731997794083466,
                   -0.707678784846507, -0.594776425699584, -0.542182374657374, -0.477652051223985, -0.414002394497506,
                   -0.326351540865686, -0.301458382421319, -0.143486910424499, -0.0878464728184052, -0.0350835941699658,
                   0.0334396260398352, 0.0795033683251447, 0.202974351567305, 0.237382785959596, 0.288908922672592,
                   0.419851917880386, 0.441532730387388, 0.499570508388721, 0.577394288619662, 0.629734626483965,
                   0.690534081997171, 0.868883439039411, 0.911733893303862, 0.940260537535768, 0.962286449219438],
                  [1.61070071922315, 2.00134259950511, 2.52365719332252, 2.33863055618848, 2.46787274461421,
                   2.92596278963705, 4.49457749339454, 5.01302648557115, 5.53887922607839, 5.59614305167494,
                   5.3790027966219, 4.96873291187938, 3.56249278950514, 2.31744895283007, 2.39921966442751,
                   1.52592143155874, 1.42166345066052, 1.19058953217964, 1.23598301133586, 0.461229833080578,
                   0.940922128674924, 0.73146046340835, 0.444386541739061, 0.332335616103906, 0.285195114684272,
                   0.219953363135822, 0.234575259776606, 0.228396325882262, 0.451944920264431, 0.655793276158532]])
    print(fit_rational(X))
    print(fit_rational_normed(X))


if __name__ == '__main__':
    main()
