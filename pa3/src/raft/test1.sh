#!/bin/zsh

for ((i=0;i<50;i++)) {
	go test -run Election
}
