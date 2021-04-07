import math
from matplotlib import pyplot as plt

F = lambda x: x ** 2 + (x ** 2 - 3 * x + 10) / (2 + x)
DF = lambda x: 2 * x + ((2 * x - 3) * (2 + x) - (x ** 2 - 3 * x + 10)) / ((2 + x) ** 2)
DDF = lambda x: 2 + ((2 * x + 6) * (x + 2) ** 2 - 2 * (2 + x) * (x ** 2 - 6 * x - 16)) / (2 + x) ** 4
LOWER_BOUND = -1
UPPER_BOUND = 5
EPSILON = 10 ** -6
K = 50
T = (3 - math.sqrt(5)) / 2
X0 = 2
CONST = 3.58254399930370


def check_validity(function, *args):
    if function == generic_bisect or function == generic_gs:
        l, u, eps, k = args
        if l >= u:
            raise ValueError("Must enter valid segment")
        elif eps < 0:
            raise ValueError("Epsilon must be a positive number")
        elif k <= 0 or not isinstance(k, int):
            raise ValueError("K number of moves must be a positive integer")
    elif function == generic_newton:
        x0, eps, k = args
        from numbers import Number
        if not isinstance(x0, Number):
            raise TypeError("x0 must be a rational number from the specified domain")
        elif eps < 0:
            raise ValueError("Epsilon must be a positive number")
        elif k <= 0 or not isinstance(k, int):
            raise ValueError("K must be a positive integer")
    elif function == generic_hybrid:
        l, u, eps1, eps2, k = args
        if l >= u:
            raise ValueError("Must enter valid segment")
        elif eps1 < 0 or eps2 < 0:
            raise ValueError("Epsilon must be a positive number")
        elif k <= 0 or not isinstance(k, int):
            raise ValueError("K number of moves must be a positive integer")


def generic_bisect_action(f, df, l, u, eps, k, fv: list):
    x = (u + l) / 2
    fv.append(f(x))
    if abs(u - l) < eps or k == 0:
        return x, fv
    if df(x) == 0:
        return x, fv
    return generic_bisect_action(f, df, l, x, eps, k - 1, fv) if df(u) * df(x) > 0 else generic_bisect_action(f, df, x,
                                                                                                              u, eps,
                                                                                                              k - 1, fv)


def generic_bisect(f, df, l, u, eps, k):
    check_validity(generic_bisect, l, u, eps, k)
    return generic_bisect_action(f, df, l, u, eps, k, [])


def generic_newton_action(f, df, ddf, x0, eps, k, fv: list):
    new_x0 = x0 - df(x0) / ddf(x0)
    fv.append(f(new_x0))
    if k < 0 or df(new_x0) < eps:
        return new_x0, fv
    return generic_newton_action(f, df, ddf, new_x0, eps, k - 1, fv)


def generic_newton(f, df, ddf, x0, eps, k):
    check_validity(generic_newton, x0, eps, k)
    return generic_newton_action(f, df, ddf, x0, eps, k, [])


def generic_hybrid(f, df, ddf, l, u, eps1, eps2, k):
    check_validity(generic_hybrid, l, u, eps1, eps2, k)
    fv = []
    x = u
    while abs(u - l) > eps1 and abs(df(x)) > eps2:
        new_x = x - df(x) / ddf(x)
        x = new_x if l < new_x < u and abs(df(new_x)) < 0.99 * abs(df(x)) else (l + u) / 2
        fv.append(f(x))
        if df(x) * df(u) > 0:
            u = x
        else:
            l = x
    return x, fv


def generic_gs(f, l, u, eps, k):
    check_validity(generic_gs, l, u, eps, k)
    fv = []
    x2 = l + T * (u - l)
    x3 = l + (1 - T) * (u - l)
    while abs(u - l) >= eps and k >= 0:
        if f(x2) < f(x3):
            u = x3
            x3 = x2
            x2 = l + T * (u - l)
        else:
            l = x2
            x2 = x3
            x3 = l + (1 - T) * (u - l)
        k -= 1
        fv.append(f((u + l) / 2))
    return (u + l) / 2, fv


def main():
    x, fv = generic_bisect(F, DF, LOWER_BOUND, UPPER_BOUND, EPSILON, K)
    print(f'Bisect:\nx: {x}\nfv: {fv}\n')
    fv = [x - CONST for x in fv]
    plt.semilogy(range(len(fv)), fv)
    plt.show()

    x, fv = generic_newton(F, DF, DDF, 2, EPSILON, K)
    print(f'Newton:\nx: {x}\nfv: {fv}\n')
    fv = [x - CONST for x in fv]
    plt.semilogy(range(len(fv)), fv)
    plt.show()

    x, fv = generic_hybrid(F, DF, DDF, LOWER_BOUND, UPPER_BOUND, EPSILON, EPSILON, K)
    print(f'Hybrid:\nx: {x}\nfv: {fv}\n')
    fv = [x - CONST for x in fv]
    plt.semilogy(range(len(fv)), fv)
    plt.show()

    x, fv = generic_gs(F, LOWER_BOUND, UPPER_BOUND, EPSILON, K)
    print(f'GS:\nx: {x}\nfv: {fv}\n')
    fv = [x - CONST for x in fv]
    plt.semilogy(range(len(fv)), fv)
    plt.show()


if __name__ == '__main__':
    main()
