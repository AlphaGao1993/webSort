package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var startTime time.Time

func InitTime() {
	startTime = time.Now()
}

func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {

	out := make(chan int, 1024)

	go func() {
		var a []int
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("read done", time.Now().Sub(startTime))
		sort.Ints(a)

		fmt.Println("InMemSort done,", time.Now().Sub(startTime))
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		fmt.Println("merge done", time.Now().Sub(startTime))
		close(out)
	}()
	return out
}

func ReadSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		byteRead := 0
		for {
			v, err := reader.Read(buffer)
			byteRead += v
			if v > 0 {
				out <- int(binary.BigEndian.Uint64(buffer))
			}
			if err != nil ||
				(chunkSize != -1 && byteRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

func WriteSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		if _, err := writer.Write(buffer); err != nil {
			fmt.Println("err, stop")
		}
	}
}

func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	n := len(inputs) / 2
	return Merge(
		MergeN(inputs[:n]...),
		MergeN(inputs[n:]...))
}
