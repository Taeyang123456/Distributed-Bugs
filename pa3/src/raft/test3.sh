#!/bin/zsh

for ((i=0;i<10;i++)) {

	echo "round: " $i

	go test -run Persist1

	go test -run Persist2

	go test -run Persist3
}
