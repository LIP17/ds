package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

var lspClient lsp.Client
var clientName string

func makeConnection(hostport string) error {
	params := lsp.NewParams()

	var err error

	lspClient, err = lsp.NewClient(hostport, params)

	if err != nil {
		return errors.New("Connect Error")
	}
	return nil
}

func writeRequest(data string, maxNonce uint64) error {
	reqMsg := bitcoin.NewRequest(data, 0, maxNonce)
	buf, err := json.Marshal(reqMsg)
	if err != nil {
		return errors.New("Write Error : Marshal Problem")
	}

	err = lspClient.Write(buf)
	if err != nil {
		return errors.New("Write Error : Cannot Write to Server")
	}

	return nil
}

func readResult() {
	// read part
	buf, err := lspClient.Read()

	if err != nil {
		printDisconnected()
		return
	}

	resultMsg := &bitcoin.Message{}
	err = json.Unmarshal(buf, resultMsg)
	if err != nil {
		fmt.Println("error when unmarshalling")
		return
	}

	printResult(strconv.FormatUint(resultMsg.Hash, 10), strconv.FormatUint(resultMsg.Nonce, 10))
}

func main() {

	// TODO: implement this!
	const numArgs = 5
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce> <clientName>")
		return
	}
	// check the argument input
	hostport := os.Args[1]
	data := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Println("Error input args[3] max nonce")
		return
	}

	clientName = os.Args[4]

	fmt.Println("Usage: ", os.Args[1], os.Args[2], os.Args[3], os.Args[4])

	// connect
	connectError := makeConnection(hostport)
	if connectError != nil {
		fmt.Println("Cannot create client or connect to server")
		return
	}

	// write request
	writeError := writeRequest(data, maxNonce)
	if writeError != nil {
		fmt.Println("Write to server error")
		return
	}

	readResult()

}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce, clientName)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected: ", clientName)
}
