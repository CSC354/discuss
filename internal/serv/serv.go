package serv

import (
	"log"
	"net"

	"github.com/CSC354/discuss/internal/discuss"
	"github.com/CSC354/discuss/pdiscuss"
	"github.com/CSC354/sijl/pkg/mamar"
	"github.com/CSC354/sijl/pkg/wathiq"
	"google.golang.org/grpc"
)

func StartDiscussServer() error {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	discussDb, err := mamar.ConnectDB("QAIDA")
	if err != nil {
		log.Fatal(err)
	}
	wathq, conn, err := wathiq.NewWathiqStub()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	dicsuss := discuss.Discuss{DB: discussDb, WathiqClient: wathq}
	pdiscuss.RegisterDiscussServer(grpcServer, dicsuss)

	err = grpcServer.Serve(lis)
	return err
}
