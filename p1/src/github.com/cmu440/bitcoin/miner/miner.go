package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"math"
	"os"
)

var lspClient lsp.Client

func makeConnection(hostport string) error {
	params := lsp.NewParams()

	var err error

	lspClient, err = lsp.NewClient(hostport, params)

	if err != nil {
		return errors.New("Connect Error")
	}
	return nil
}

func writeToServer(msg *bitcoin.Message) error {

	buf, err := json.Marshal(msg)

	if err != nil {
		return errors.New("Write Error : Marshal Problem")
	}

	err = lspClient.Write(buf)

	if err != nil {
		return errors.New("Write Error : cannot write")
	}

	return nil
}

func main() {
	// TODO: implement this!
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	hostport := os.Args[1]

	// connection
	err := makeConnection(hostport)
	if err != nil {
		fmt.Println("cannot create client or connect to server")
		return
	}

	// write join request
	joinMsg := bitcoin.NewJoin()
	err = writeToServer(joinMsg)
	if err != nil {
		fmt.Println("Write join request problem")

		return
	}

	handleWork()

}

func calculateMinHash(data string, lower, upper uint64) (minHash, nonce uint64) {
	minHash = math.MaxUint64
	nonce = 0
	for i := lower; i <= upper; i++ {
		hash := bitcoin.Hash(data, i)
		if hash < minHash {
			minHash = hash
			nonce = i
		}
	}

	return
}

func handleWork() {
	for {
		buf, err := lspClient.Read()
		if err != nil {
			fmt.Println("Connection Lost")
			return
		}

		reqMsg := &bitcoin.Message{}

		err = json.Unmarshal(buf, reqMsg)

		if err != nil {
			fmt.Println("Working Error : Unmarshaling Error")
			return
		}

		hash, nonce := calculateMinHash(reqMsg.Data, reqMsg.Lower, reqMsg.Upper)
		resultMsg := bitcoin.NewResult(hash, nonce)

		writeToServer(resultMsg)

	}
}
