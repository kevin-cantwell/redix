package main

import (
	"fmt"
	"net"
	"os"
	"strings"

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

		cmd := parseRESP(clientResp)
		switch cmd.Name {
		case "PROMOTE":
			if err := handlePromotion(proxy, cmd); err != nil {
				continue
			}
			return
		case "MONITOR":
		default:
		}

		if err := proxy.WriteServerObject(clientResp); err != nil {
			return
		}
	}
}

// Parses the resp object and returns the components
func parseRESP(resp []byte) redix.Command {
	if strings.ToUpper(string(resp)) == "*3\r\n$7\r\nPROMOTE\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n" {
		return redix.Command{Name: "PROMOTE", Args: []string{"127.0.0.1", "6380", ""}}
	}
	if strings.ToUpper(string(resp)) == "*1\r\n$7\r\nMONITOR\r\n" {
		return redix.Command{Name: "MONITOR"}
	}
	return redix.Command{Name: "?"}
}

func handlePromotion(proxy redix.Proxy, cmd redix.Command) error {
	// For now hard-coding promotion deets :|

	ip := cmd.Args[0]
	port := cmd.Args[1]
	auth := cmd.Args[2]
	if err := proxy.Promote(ip, port, auth); err != nil {
		return err
	}

	return nil
}

// func handleProxy(ctx context.Context, proxy redix.Proxy, serverURL string) {
// 	defer proxy.Close()

// 	if err := proxy.Open(serverURL); err != nil {
// 		proxy.WriteClientErr(err)
// 		return
// 	}

// 	// Handle client requests, but die if kill signal is sent.
// 	for {
// 		select {
// 		// Kill signal received
// 		case <-ctx.Done():
// 			return
// 		// Client sends a request
// 		case clientResp := <-proxy.ReadClientObject():
// 			// Handle client errors
// 			if clientResp.Err != nil {
// 				if clientResp.Err == io.EOF {
// 					proxy.Println("client sent EOF")
// 					return
// 				}
// 				if err := proxy.WriteClientErr(clientResp.Err); err != nil {
// 					return
// 				}
// 				continue
// 			}

// 			// Intercept any extended commands, such as PROMOTE
// 			if strings.ToUpper(string(clientResp.Body)) == "*2\r\n$7\r\nPROMOTE\r\n$6\r\nSLAVE0\r\n" {
// 				proxy.Println("promoting slave0")
// 				if err := proxy.Promote("slave0"); err != nil {
// 					if err := proxy.WriteClientErr(err); err != nil {
// 						return
// 					}
// 				}
// 				continue
// 			}
// 			// case "*1\r\n$7\r\nMONITOR\r\n":
// 			// 	monitor = true
// 			// }

// 			select {
// 			// Kill signal received
// 			case <-ctx.Done():
// 				return
// 			// Forward client request to server and wait for response
// 			case serverResp := <-proxy.WriteServerObject(clientResp.Body):
// 				// Handle server errors
// 				if serverResp.Err != nil {
// 					if serverResp.Err == io.EOF {
// 						proxy.Println("server sent EOF")
// 						return
// 					}
// 					if err := proxy.WriteClientErr(serverResp.Err); err != nil {
// 						return
// 					}
// 					continue
// 				}

// 				if err := proxy.WriteClientObject(serverResp.Body); err != nil {
// 					return
// 				}
// 			}
// 		}
// 	}
// }
