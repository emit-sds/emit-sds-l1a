#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 30 14:57:21 2022

@author: bradley
"""

from pathlib import Path
import sys

import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
import spectral.io.envi as envi

filename = sys.argv[1]

data_raw = envi.open(Path(filename).with_suffix('.hdr'))
waterfall = np.mean(data_raw[:, :, :], axis=2)
waterfall = waterfall / np.max(waterfall)

plt.figure(figsize=(11, 8.5))
plt.clf()
im = plt.imshow(waterfall)
plt.title('waterfall plot, average over spectral pixels')
plt.xlabel('spatial pixel')
plt.ylabel('frame')
divider = make_axes_locatable(plt.gca())
cax = divider.append_axes("right", size="5%", pad=0.05)
plt.colorbar(im, cax=cax)
plt.savefig(filename.replace('.img', '_waterfall.png'))
plt.close()
