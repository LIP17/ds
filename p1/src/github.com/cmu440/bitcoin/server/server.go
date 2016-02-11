package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"math"
	"os"
	"strconv"
)

const (
	name                   = "log.txt"
	flag                   = os.O_RDWR | os.O_CREATE
	perm                   = os.FileMode(0666)
	MinerWorkloadThreshold = 10000
)

type workUnit struct {
	clientWorkFor int // work for this client ID
	workMsg       *bitcoin.Message
}

type clientStatus struct {
	notFinishedJob int
	minHash        uint64
	nonce          uint64
}

var (
	minerCurrWork   map[int]*workUnit
	unOccupiedMiner map[int]bool
	clients         map[int]*clientStatus
	jobQ            *list.List
	lspServer       lsp.Server
	FLOG            *log.Logger
)

func newClientStatus(totalJobNum int) *clientStatus {
	return &clientStatus{
		notFinishedJob: totalJobNum,
		minHash:        math.MaxUint64,
		nonce:          0,
	}
}

func sendMessage(connId int, msg *bitcoin.Message) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = lspServer.Write(connId, buf)

	if err != nil {
		return err
	}

	return nil
}

func partitionJob(connId int, msg *bitcoin.Message) {
	data, lower, upper := msg.Data, msg.Lower, msg.Upper
	if upper-lower+1 <= MinerWorkloadThreshold {
		workU := &workUnit{
			clientWorkFor: connId,
			workMsg:       bitcoin.NewRequest(data, lower, upper),
		}
		jobQ.PushBack(workU)
		clients[connId] = newClientStatus(1)
	} else {
		// if there is no miner, partition the job as if
		// there is one miner
		numOfMiner := len(minerCurrWork)
		if numOfMiner == 0 {
			numOfMiner = 1
		}

		clients[connId] = newClientStatus(numOfMiner)

		interval := (upper - lower + 1) / uint64(numOfMiner)

		for i := 0; i < numOfMiner; i++ {
			currLower := lower + uint64(i)*interval
			currUpper := currLower + interval - 1
			if i == numOfMiner-1 {
				currUpper = upper
			}

			workU := &workUnit{
				clientWorkFor: connId,
				workMsg:       bitcoin.NewRequest(data, currLower, currUpper),
			}
			jobQ.PushBack(workU)
		}

	}

}

func assignJobsUnoccupiedMiner() {
	if jobQ.Len() > 0 {
		for connId, _ := range unOccupiedMiner {
			// choose the first work unit that works for a active client
			for jobQ.Len() > 0 {
				workU := jobQ.Front().Value.(*workUnit)

				if clients[workU.clientWorkFor] != nil {
					// write the request to miner
					err := sendMessage(connId, workU.workMsg)

					if err == nil {
						jobQ.Remove(jobQ.Front())
						minerCurrWork[connId] = workU
						delete(unOccupiedMiner, connId)
						break
					}

				} else {
					jobQ.Remove(jobQ.Front())
				}
			}

			if jobQ.Len() == 0 {
				break
			}

		}
	}
}

func handleIncomeMessage(connId int, payLoad []byte) {
	msg := &bitcoin.Message{}
	err := json.Unmarshal(payLoad, msg)
	if err != nil {
		fmt.Println("Unmarshall incoming message error !")
		return
	}

	FLOG.Println("handle income message: ", msg)

	switch msg.Type {
	case bitcoin.Join:
		minerCurrWork[connId] = nil
		unOccupiedMiner[connId] = true
	case bitcoin.Request:
		if clients[connId] == nil {
			partitionJob(connId, msg)
		}
	case bitcoin.Result:
		updateResult(connId, msg)
	}

}

func updateResult(connId int, msg *bitcoin.Message) {
	workU := minerCurrWork[connId]

	minerCurrWork[connId] = nil
	unOccupiedMiner[connId] = true

	clientStatus := clients[workU.clientWorkFor]
	// if the client has already lost connection, just ignore its result message
	if clientStatus != nil {
		// refresh the current client status
		clientStatus.notFinishedJob -= 1
		if msg.Hash < clientStatus.minHash {
			clientStatus.minHash = msg.Hash
			clientStatus.nonce = msg.Nonce
		}

		// if all partitioned jobs of the client's request are done, return the result back to the client
		if clientStatus.notFinishedJob == 0 {
			resultMsg := bitcoin.NewResult(clientStatus.minHash, clientStatus.nonce)
			FLOG.Println("Send back results", resultMsg.Hash, resultMsg.Nonce)
			sendMessage(workU.clientWorkFor, resultMsg)
			delete(clients, workU.clientWorkFor)
		}
	}
}

func handleFailure(connId int) {
	// if a miner losts connection
	if currWork, ok := minerCurrWork[connId]; ok {
		// get the miner's work back to queue's front
		if currWork != nil {
			jobQ.PushFront(currWork)
		}

		// delete the work of this miner
		delete(unOccupiedMiner, connId)
		delete(minerCurrWork, connId)
	} else if clients[connId] != nil {
		// if this id is a client
		// delete the lost connection client
		delete(clients, connId)
	}
}

func main() {
	// TODO: implement this!
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	port, err := strconv.Atoi(os.Args[1])

	if err != nil {
		fmt.Println("Input Server Port error")
		return
	}

	params := lsp.NewParams()

	lspServer, err = lsp.NewServer(port, params)
	if err != nil {
		fmt.Println("cannot build the server")
		return
	}

	fmt.Println("Listen on ", port)
	// fmt.Println(lspServer.Read())

	//
	minerCurrWork = make(map[int]*workUnit)
	unOccupiedMiner = make(map[int]bool)
	clients = make(map[int]*clientStatus)

	jobQ = list.New()

	// build a logger
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		fmt.Println("Error Build the LOG file")
		return
	}

	defer file.Close()

	FLOG = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	for {
		assignJobsUnoccupiedMiner()
		connId, payLoad, err := lspServer.Read()
		if err != nil {
			FLOG.Println("Handle failure", connId)
			handleFailure(connId)
		} else {
			handleIncomeMessage(connId, payLoad)
		}

	}

}
