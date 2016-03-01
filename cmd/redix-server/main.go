package main

import (
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
		array, err := proxy.ParseClientObject()
		if err != nil {
			// proxy.Println("client:", err)
			proxy.WriteClientErr(err)
			return
		}

		switch strings.ToLower(array[0].String()) {
		// PROMOTE slaveX auth timeout
		case "promote":
			if len(array) < 3 || len(array) > 4 {
				proxy.WriteClientErr(errors.New("wrong number of arguments for 'promote' command"))
				return
			}
			slaveID, auth, timeout := array[1].String(), "", array[len(array)-1].String()
			if len(array) == 4 {
				auth = array[2].String()
			}
			if err := proxy.Promote(slaveID, auth, timeout); err != nil {
				proxy.WriteClientErr(err)
			}
			return
		default:
		}

		if err := proxy.WriteServerObject(array.Raw()); err != nil {
			proxy.WriteClientErr(err)
			return
		}
	}
}
