package main

import (
	"github.com/CSC354/discuss/internal/serv"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	serv.StartDiscussServer()
}
