import numpy as np


def line_from_points(p: tuple, q: tuple):
    # Returns gradient and y-intercept of straight
    # line given two points on the line. If the
    # gradient is infinite, the x-intercept is
    # returned instead.
    if p[0] == q[0] or np.inf in [p[0], q[0]]:
        return np.inf, p[0]
    else:
        m = (p[1] - q[1])/(p[0] - q[0])
        return m, p[1] - m*p[0]


def line_from_grad(m: float, p: tuple):
    # Returns gradient and y-intercept of straight
    # line given two points on the line. If the
    # gradient is infinite, the x-intercept is
    # returned instead.
    if m == np.inf:
        return m, p[0]
    else:
        return m, p[1] - m*p[0]


def midpoint(p: tuple, q: tuple):
    # Returns the midpoint of the straight line
    # through points p and q.
    return 0.5 * (p[0] + q[0]), 0.5 * (p[1] + q[1])


def line_intersection(line1: tuple, line2: tuple):
    # Returns the intersection of two lines.
    # Lines should be given in the form
    # (gradient, y-intercept). If the gradient is
    # infinite, use the x-intercept instead of the
    # y-intercept.
    m1 = line1[0]
    c1 = line1[1]
    m2 = line2[0]
    c2 = line2[1]
    if (m1 == m2) and (c1 == c2):
        raise Exception("These lines are the same.")
    elif m1 == m2:
        return np.inf, np.inf
    elif m1 == np.inf:
        return c1, m2*c1 + c2
    elif m2 == np.inf:
        return c2, m1*c2 + c1
    else:
        x = (c2 - c1)/(m1 - m2)
        return x, m1*x + c1


def perp_grad(m):
    if m == np.inf:
        return 0
    elif m == 0:
        return np.inf
    else:
        return -1/m


def perp_bisector(p: tuple, q: tuple):
    m, _ = line_from_points(p, q)
    return line_from_grad(perp_grad(m), midpoint(p, q))


def circumcentre(p: tuple, q: tuple, r: tuple):
    perp1 = perp_bisector(p, q)
    perp2 = perp_bisector(p, r)
    return line_intersection(perp1, perp2)


def d2(p: tuple, q: tuple):
    if (np.inf in p) or (np.inf in q):
        return np.inf
    return (p[0] - q[0])**2 + (p[1] - q[1])**2


def in_circle(centre: tuple, radius_squared: float, p: tuple, tol: float = 10**(-10)):
    diff = radius_squared - d2(centre, p)
    if np.abs(diff) < tol:
        return 0
    else:
        return np.sign(diff)


def point_equal(p: tuple, q: tuple, tol: float = 10**(-10)):
    return d2(p, q) < tol


def point_in_set(p: tuple, points: list, tol: float = 10**(-10)):
    for point in points:
        if d2(p, point) < tol:
            return True
    else:
        return False


def point_on_line(m: float, c: float, p: tuple, tol: float = 10**(-10)):
    if m == np.inf:
        return np.abs(p[0] - c) < tol or p[1] == np.inf
    if p[0] == np.inf:
        return True
    return np.abs(p[1] - m*p[0] - c) < tol


def nearest_point(p: tuple, points: list):
    candidate = points[0]
    for point in points[1:]:
        if d2(p, point) < d2(p, candidate):
            candidate = point
    return candidate, points.index(candidate)
