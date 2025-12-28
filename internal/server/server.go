package server

import (
	"io"
	"log"
	"mtredis/internal/config"
	"mtredis/internal/constant"
	"mtredis/internal/core"
	"mtredis/internal/core/io_multiplexing"
	"net"
	"syscall"
	"time"
)

func RunIOMultiplexingServer() {
	listener, err := net.Listen(config.Protocol, config.Port)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("starting an i/o multiplexing tcp server on port", config.Port)
	defer listener.Close()

	// check whether the listener is actually a TCP listener
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		log.Fatal("listener is not a tcp listener")
	}

	listenerFile, err := tcpListener.File()
	if err != nil {
		log.Fatal(err)
	}
	defer listenerFile.Close()

	// get the file descriptor from the listener
	serverFd := int(listenerFile.Fd())

	// create an I/O Multiplexer instance
	ioMultiplexer, err := io_multiplexing.CreateIOMultiplexer()
	if err != nil {
		log.Fatal(err)
	}
	defer ioMultiplexer.Close()

	// monitor "read" events on the server's file descriptor
	if err = ioMultiplexer.Monitor(io_multiplexing.Event{
		Fd: serverFd,
		Op: io_multiplexing.OpRead,
	}); err != nil {
		log.Fatal(err)
	}

	var events = make([]io_multiplexing.Event, config.MaxConnection)
	var lastActiveDeleteExpiredKeys = time.Now()

	for {
		// check for the last active delete expired keys
		// if the last execution is more than 100ms before, do it
		if time.Now().After(lastActiveDeleteExpiredKeys.Add(constant.ActiveDeleteFrequency)) {
			core.ActiveDeleteExpiredKeys()
			lastActiveDeleteExpiredKeys = time.Now()
		}

		// wait for file descriptors in the monitoring list to be ready for I/O
		// this is a blocking call
		events, err = ioMultiplexer.Wait()
		if err != nil {
			continue
		}

		// go through those file descriptors
		for i := 0; i < len(events); i++ {
			if events[i].Fd == serverFd { // a new client want to connect
				log.Printf("new client is trying to connect")
				// set up new connection
				connFd, _, err := syscall.Accept(serverFd)
				if err != nil {
					log.Printf("failed to accept the connection: %v", err)
					continue
				}
				log.Printf("set up a new connection")

				// ask epoll to monitor this connection
				if err = ioMultiplexer.Monitor(io_multiplexing.Event{
					Fd: connFd,
					Op: io_multiplexing.OpRead,
				}); err != nil {
					log.Fatal(err)
				}
			} else { // an existing client sends a new command
				cmd, err := readCommand(events[i].Fd)
				if err != nil {
					if err == io.EOF || err == syscall.ECONNRESET {
						log.Println("client disconnected")
						_ = syscall.Close(events[i].Fd)
						continue
					}

					log.Printf("read error: %v", err)
					continue
				}

				if err = core.ExecuteAndResponse(cmd, events[i].Fd); err != nil {
					log.Printf("write error: %v", err)
				}
			}
		}
	}
}

func readCommand(fd int) (*core.Command, error) {
	var buf = make([]byte, 512)
	n, err := syscall.Read(fd, buf)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, io.EOF
	}

	return core.ParseCmd(buf[:n])
}
