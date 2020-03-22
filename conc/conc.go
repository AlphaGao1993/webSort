package main

import "fmt"

func main() {
	go printHelloWorld()
}

func printHelloWorld() {
	fmt.Println("hello world")
}