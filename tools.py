#!/usr/bin/python3

from pathlib import Path

# listing subdirectories
p = Path('.')
lsd = [x for x in p.iterdir() if x.is_dir()]

# navigating inside a directory tree

p = Path('./etc')

f = p / 'subetc' / 'reboot'

f.resolve()

# opening a file:
with f.open() as f: f.readline()
