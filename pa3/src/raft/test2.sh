#!/bin/zsh

for ((i = 0; i< 30 ;i++)) {

	echo "round : " $i	

	go test -run BasicAgree

	go test -run FailAgree

	go test -run FailNoAgree

	go test -run ConcurrentStarts

	go test -run Rejoin

	go test -run Backup

	go test -run Count
}