package main

import "mtredis/internal/server"

func main() {
	server.RunIOMultiplexingServer()
}
