package pipeline

import (
	"bufio"
	"net"
)

func NetworkSink(address string, in <-chan int) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	go func() {
		defer listener.Close()

		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		writer := bufio.NewWriter(conn)
		defer writer.Flush()
		WriteSink(writer, in)
	}()
}

func NetworkSource(addr string) <-chan int {
	out := make(chan int)

	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		reader := bufio.NewReader(conn)
		readSource := ReadSource(reader, -1)
		for v := range readSource {
			out <- v
		}
		close(out)
	}()
	return out
}
