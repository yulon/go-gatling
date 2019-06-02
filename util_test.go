package gatling

import (
	"fmt"
	"testing"
)

func TestIDAppender(*testing.T) {
	ida := newIDAppender(func(iads []idAndData) {
		for i, v := range iads {
			if int(v.id) != i+1 {
				panic("v.id != i")
			}
		}
	})
	if ida.TryAdd(8, nil) == false {
		panic("ida.TryAdd onec")
	}
	if ida.TryAdd(8, nil) == true {
		panic("ida.TryAdd twice")
	}
	ida.TryAdd(3, nil)
	ida.TryAdd(2, nil)
	ida.TryAdd(4, nil)
	ida.TryAdd(1, nil)
}

func TestConn(*testing.T) {
	pr, err := Listen("localhost:8828+")
	if err != nil {
		panic(err)
	}
	go func() {
		con, err := pr.AcceptGatling()
		if err != nil {
			panic(err)
		}

		data, err := con.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Println("server recv:", string(data))

		fmt.Println("server send: 246")
		con.Send([]byte("246"))

		_, err = con.Read(data)
		if err != nil {
			panic(err)
		}
		fmt.Println("server read:", string(data))

		fmt.Println("server write: 000")
		con.Write([]byte("000"))
	}()

	con, err := Dial("127.0.0.1:8828")
	if err != nil {
		panic(err)
	}

	fmt.Println("client send: 123")
	con.Send([]byte("123"))

	data, err := con.Recv()
	if err != nil {
		panic(err)
	}
	fmt.Println("client recv:", string(data))

	fmt.Println("client write: 111")
	con.Write([]byte("111"))

	_, err = con.Read(data)
	if err != nil {
		panic(err)
	}
	fmt.Println("client read:", string(data))
}
