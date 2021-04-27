from matplotlib import pyplot as plt
import numpy as np


def generic_gs(f, l, u, eps, k):
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
    return (l + u) / 2, fv


def gs_denoise_step(mu, a, b, c):
    f = lambda x: mu * (x - a) ** 2 + abs(x - b) + abs(x - c)
    u = max(a, b, c) + 1
    l = min(a, b, c) - 1
    return generic_gs(f, l, u, 10 ** -10, float('inf'))[0]


def gs_denoise(s, alpha, N):
    x = np.copy(s)
    for i in range(N + 1):
        for k in range(len(x)):
            if k == 0:
                x[k] = gs_denoise_step(2*alpha, s[k], x[k + 1], x[k + 1])
            elif k == len(x) - 1:
                x[k] = gs_denoise_step(2*alpha, s[k], x[k - 1], x[k - 1])
            else:
                x[k] = gs_denoise_step(alpha, s[k], x[k - 1], x[k + 1])
    return x


def main():
    # plotting the real discrete signal
    real_s_1 = [1.] * 40
    real_s_0 = [0.] * 40

    plt.plot(range(40), real_s_1, 'black', linewidth=0.7)
    plt.plot(range(41, 81), real_s_0, 'black', linewidth=0.7)

    # solving the problem
    s = np.array([[1.] * 40 + [0.] * 40]).T + 0.1 * np.random.randn(80, 1)  # noised signal
    x1 = gs_denoise(s, 0.5, 100)
    x2 = gs_denoise(s, 0.5, 1000)
    x3 = gs_denoise(s, 0.5, 10000)

    plt.plot(range(80), s, 'cyan', linewidth=0.7)
    plt.plot(range(80), x1, 'red', linewidth=0.7)
    plt.plot(range(80), x2, 'green', linewidth=0.7)
    plt.plot(range(80), x3, 'blue', linewidth=0.7)

    plt.show()


if __name__ == '__main__':
    main()
