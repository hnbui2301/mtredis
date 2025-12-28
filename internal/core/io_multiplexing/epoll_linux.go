package io_multiplexing

import (
	"log"
	"mtredis/internal/config"
	"syscall"
)

type Epoll struct {
	Fd            int
	EpollEvents   []syscall.EpollEvent
	GenericEvents []Event
}

func CreateIOMultiplexer() (*Epoll, error) {
	epollFd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Printf("failed to create epoll: %v", err)
		return nil, err
	}

	return &Epoll{
		Fd:            epollFd,
		EpollEvents:   make([]syscall.EpollEvent, config.MaxConnection),
		GenericEvents: make([]Event, config.MaxConnection),
	}, nil
}

func (ep *Epoll) Wait() ([]Event, error) {
	n, err := syscall.EpollWait(ep.Fd, ep.EpollEvents, -1)
	if err != nil {
		log.Printf("failed to handle event: %v", err)
		return nil, err
	}

	for i := 0; i < n; i++ {
		ep.GenericEvents[i] = createEvent(ep.EpollEvents[i])
	}

	return ep.GenericEvents[:n], nil
}

func (ep *Epoll) Monitor(e Event) error {
	epollEvent := e.toNative()

	// Add event's file descriptor to the monitoring list of the epoll
	return syscall.EpollCtl(ep.Fd, syscall.EPOLL_CTL_ADD, e.Fd, &epollEvent)
}

func (ep *Epoll) Close() error {
	return syscall.Close(ep.Fd)
}

func (e Event) toNative() syscall.EpollEvent {
	var event uint32 = syscall.EPOLLIN
	if e.Op == OpWrite {
		event = syscall.EPOLLOUT
	}

	return syscall.EpollEvent{
		Events: event,
		Fd:     int32(e.Fd),
	}
}

func createEvent(ep syscall.EpollEvent) Event {
	var op Operation = OpRead
	if ep.Events == syscall.EPOLLOUT {
		op = OpWrite
	}

	return Event{
		Fd: int(ep.Fd),
		Op: op,
	}
}
