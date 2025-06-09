#!/bin/sh

esbuild \
	./index.mjs \
	--bundle \
	--minify \
	--platform=browser \
	--format=esm \
	--target=esnext \
	--outfile=./dist/index-min.mjs

esbuild \
	./index.mjs \
	--bundle \
	--platform=browser \
	--format=esm \
	--target=esnext \
	--outfile=./dist/index.mjs
