package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ushmodin/avaxo/goavaxo/client"
)

const ConnectionString = "amqp://avaxo:avaxo@localhost:5672/"

func main() {
	if len(os.Args) != 2 || os.Args[1] == "--help" || os.Args[1] == "-h" {
		usage()
		return
	}
	name := os.Args[1]
	cl, err := avaxo.NewClient(ConnectionString, name)
	if err != nil {
		log.Fatal(err)
	}
	for {
		err := cl.GetSettingsFromServer()
		if err == nil {
			break
		}
		log.Printf("Can't get agent settings %s", err)
		<-time.After(60 * time.Second)
		continue
	}
	for {
		err := cl.ListenCommandQueue()
		if err != nil {
			log.Printf("Error while listen command queue %s", err)
		}
		<-time.After(30 * time.Second)
	}
}

func usage() {
	fmt.Printf("Run avaxo <agent name>\n")
}
