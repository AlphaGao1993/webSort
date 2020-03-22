package main

import (
	"bufio"
	"fmt"
	"goSort/pipeline"
	"os"
	"time"
)

func main() {
	fmt.Println(time.Now())
	const filename = "large.in"
	// generate random count
	const count = 100000000
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	source := pipeline.RandomSource(count)
	writer := bufio.NewWriter(file)
	pipeline.WriteSink(writer, source)
	writer.Flush()

	fmt.Println(time.Now())
	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	readSource := pipeline.ReadSource(reader, -1)

	index := 0
	for v := range readSource {
		fmt.Println(v)
		index++
		if index >= 100 {
			break
		}
	}
}

func mergeDemo() {
	ch :=
		pipeline.Merge(
			pipeline.InMemSort(pipeline.ArraySource(1, 2, 5, 6, 3, 9, 2, 10)),
			pipeline.InMemSort(pipeline.ArraySource(4, 2, 7, 2, 8, 24, 19, 6)))

	for v := range ch {
		fmt.Println(v)
	}
}
