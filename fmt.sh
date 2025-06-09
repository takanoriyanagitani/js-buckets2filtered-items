#!/bin/sh

find \
	. \
	-type f \
	-name '*.mjs' |
	xargs deno fmt
