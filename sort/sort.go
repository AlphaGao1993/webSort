package main

import (
	"bufio"
	"fmt"
	"goSort/pipeline"
	"os"
	"strconv"
)

func main() {
	pip := createNetworkPipeline("large.in", 800000000, 4)
	writeToFile(pip, "large.out")
	printFile("large.out")
}

func printFile(fName string) {
	file, err := os.Open(fName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	source := pipeline.ReadSource(file, -1)
	count := 0
	for v := range source {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func writeToFile(pip <-chan int, fName string) {
	file, err := os.Create(fName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	pipeline.WriteSink(writer, pip)
}

func createPipeline(fName string, fileSize, chunkCount int) <-chan int {
	pipeline.InitTime()
	chunkSize := fileSize / chunkCount
	var sortRes []<-chan int
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fName)
		if err != nil {
			panic(err)
		}
		if _, err := file.Seek(int64(i*chunkSize), 0); err != nil {
			panic(err)
		}

		readSource := pipeline.ReadSource(bufio.NewReader(file), chunkSize)
		sortRes = append(sortRes, pipeline.InMemSort(readSource))
	}
	return pipeline.MergeN(sortRes...)
}

func createNetworkPipeline(fName string, fileSize, chunkCount int) <-chan int {
	pipeline.InitTime()
	chunkSize := fileSize / chunkCount
	var sortAddr []string
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fName)
		if err != nil {
			panic(err)
		}
		if _, err := file.Seek(int64(i*chunkSize), 0); err != nil {
			panic(err)
		}
		readSource := pipeline.ReadSource(bufio.NewReader(file), chunkSize)

		add := ":" + strconv.Itoa(7000+i)
		pipeline.NetworkSink(add, pipeline.InMemSort(readSource))
		sortAddr = append(sortAddr, add)
	}
	var sortRes []<-chan int
	for _, addr := range sortAddr {
		sortRes = append(sortRes, pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sortRes...)
}
