package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	redisURL, err := url.Parse(os.Getenv("REDIS_URL"))
	if err != nil {
		fmt.Println("Error parsing REDIS_URL")
		os.Exit(1)
	}

	ipPort := strings.Split(redisURL.Host, ":")
	var auth string
	if redisURL.User != nil {
		if password, ok := redisURL.User.Password(); ok {
			auth = password
		}
	}
	dialer := &redix.Dialer{IP: ipPort[0], Port: ipPort[1], Auth: auth}
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
			proxy.Println("client:", err)
			return
		}

		parsed, err := redix.NewReader(bytes.NewReader(clientResp)).ParseObject()
		if err != nil {
			proxy.WriteClientErr(err)
			continue
		}

		switch string(bytes.ToLower(parsed[0])) {
		case "promote":
			if len(parsed) < 3 || len(parsed) > 4 {
				proxy.WriteClientErr(errors.New("wrong number of arguments for 'promote' command"))
				return
			}
			ip, port, auth := string(parsed[1]), string(parsed[2]), ""
			if len(parsed) == 4 {
				auth = string(parsed[3])
			}
			if err := proxy.Promote(ip, port, auth); err != nil {
				proxy.WriteClientErr(err)
			}
			return
		default:
		}

		if err := proxy.WriteServerObject(clientResp); err != nil {
			proxy.WriteClientErr(err)
			return
		}
	}
}
