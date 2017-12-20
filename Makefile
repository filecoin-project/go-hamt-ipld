all: deps

gx:
	go get github.com/whyrusleeping/gx
	go get github.com/whyrusleeping/gx-go

deps: gx covertools
	gx --verbose install --global
	gx-go rewrite

publish:
	gx-go rewrite --undo

