package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/kevin-cantwell/redix"

	"golang.org/x/net/context"
)

func main() {
	port := os.Getenv("PORT")
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on " + ":" + port)

	// TODO: source this from writable config file
	dialer := &redix.Dialer{IP: "127.0.0.1", Port: "6379"}
	conns := redix.NewConnectionManager()

	ctx := context.Background()
	for {
		// Listen for an incoming connection.
		clientConn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting client connection: ", err.Error())
			continue
		}

		proxy := redix.NewProxy(clientConn, dialer, conns)
		go handle(ctx, proxy)
	}
}

func handle(ctx context.Context, proxy redix.Proxy) {
	// Make sure to close both client and proxy connections on defer
	defer proxy.Close()

	if err := proxy.Open(); err != nil {
		return
	}

	for {
		clientResp, err := proxy.ReadClientObject()
		if err != nil {
			return
		}

		parsed, err := redix.ParseRESP(clientResp)
		if err != nil {
			proxy.WriteClientErr(err)
			continue
		}

		switch string(bytes.ToLower(parsed[0])) {
		case "promote":
			if err := handlePromotion(proxy, parsed[1:]); err != nil {
				continue
			}
			return
		case "monitor":
		default:
		}

		if err := proxy.WriteServerObject(clientResp); err != nil {
			return
		}
	}
}

// Parses the resp object and returns the components
// func parseClientRESP(resp []byte) redix.Command {
// 	parsed := redix.ParseRESP(resp)
// 	if strings.ToUpper(string(resp)) == "*4\r\n$7\r\nPROMOTE\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$4\r\nPASS\r\n" {
// 		return redix.Command{Name: "PROMOTE", Args: []string{"127.0.0.1", "6380", "pass"}}
// 	}
// 	if strings.ToUpper(string(resp)) == "*1\r\n$7\r\nMONITOR\r\n" {
// 		return redix.Command{Name: "MONITOR"}
// 	}
// 	return redix.Command{Name: "?"}
// }

func handlePromotion(proxy redix.Proxy, args [][]byte) error {
	if len(args) < 2 || len(args) > 3 {
		err := errors.New("wrong number of arguments for 'promote' command")
		proxy.WriteClientErr(err)
		return err
	}
	ip, port, auth := string(args[0]), string(args[1]), ""
	if len(args) == 3 {
		auth = string(args[2])
	}
	return proxy.Promote(ip, port, auth)
}
