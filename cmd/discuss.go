package main

import (
	"github.com/CSC354/discuss/internal/serv"
	"log"
)

func main() {
	serv.StartDiscussServer()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
