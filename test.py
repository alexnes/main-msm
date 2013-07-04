#!/usr/bin/env python
# -*- coding: utf-8 -*-
from globals_description import description as d

props = {}
pr_gl = {}
for gl in d:
	#print "%s - %s" % (gl, d[gl]['name'])
	for pr in d[gl]['prop']:
		#print "\t%s:\t%s" % (pr, d[gl]['prop'][pr])
		if not props.get(pr):
			props[pr] = d[gl]['prop'][pr]
		if not pr_gl.get(pr):
			pr_gl[pr] = []
		pr_gl[pr].append(gl)

for pr in sorted(props, key = '{:>03}'.format):
	print "%s\t%s %s" % (pr, props[pr], sorted(pr_gl[pr]))
