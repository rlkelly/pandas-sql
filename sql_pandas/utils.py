#!/usr/bin/env python3


import numpy as np
from sqlalchemy import Column, Integer, String, Float


def convert_types(type, primary_key=False, maxlen=None, unique=False):
    if np.issubdtype(type, np.integer):
        return Column(Integer(), primary_key=primary_key)
    if np.issubdtype(type, np.integer):
        return Column(Float(), primary_key=primary_key)
    if np.issubdtype(type, np.object_):
        return Column(String(length=maxlen), unique=unique)
