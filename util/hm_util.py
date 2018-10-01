# This is just a holding area for high-maintenance, possibly unneeded
# functions that would require packages like NumPy to be
# imported into the ETL virtual environment.

import numpy as np

from calendar import monthrange

def centroid_np(arr):
    # Find centroid of NumPy array of coordinates.
    # For example: centroid_np(np.array([[0,1],[3.0,9]]))
    length = arr.shape[0]
    sum_x = np.sum(arr[:, 0])
    sum_y = np.sum(arr[:, 1])
    return sum_x/length, sum_y/length

