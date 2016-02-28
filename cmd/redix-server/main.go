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
			return
		}

		parsed, err := redix.NewReader(bytes.NewReader(clientResp)).ParseObject()
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
