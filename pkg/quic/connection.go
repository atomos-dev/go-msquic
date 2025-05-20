package quic

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// #include "msquic.h"
import "C"

const streamAcceptQueueSize = 1000

type Connection interface {
	OpenStream() (MsQuicStream, error)
	AcceptStream(ctx context.Context) (MsQuicStream, error)
	Close() error
	RemoteAddr() net.Addr
	Context() context.Context
}

type Config struct {
	MaxIncomingStreams            int64
	MaxIdleTimeout                time.Duration
	KeepAlivePeriod               time.Duration
	CertFile                      string
	KeyFile                       string
	Alpn                          string
	MaxBindingStatelessOperations int64
	MaxStatelessOperations        int64
	TracePerfCounts               func([]string, []uint64)
	TracePerfCountReport          time.Duration
	FailOnOpenStream              bool
	EnableDatagramReceive         bool
	DisableSendBuffering          bool
	MaxBytesPerKey                int64
}

type MsQuicConn struct {
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
	remoteAddr        net.UDPAddr
	shutdown          *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
	failOpenStream    bool
	openStream        *sync.Mutex
}

func newMsQuicConn(c C.HQUIC, failOnOpen bool) MsQuicConn {
	ctx, cancel := context.WithCancel(context.Background())

	ip, port := getRemoteAddr(c)

	return MsQuicConn{
		conn:              c,
		acceptStreamQueue: make(chan MsQuicStream, streamAcceptQueueSize),
		ctx:               ctx,
		cancel:            cancel,
		remoteAddr:        net.UDPAddr{IP: ip, Port: port},
		shutdown:          new(atomic.Bool),
		streams:           new(sync.Map),
		failOpenStream:    failOnOpen,
		openStream:        new(sync.Mutex),
	}
}

func (mqc MsQuicConn) Close() error {
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	if !mqc.shutdown.Swap(true) {
		wg := sync.WaitGroup{}
		mqc.streams.Range(func(k, v any) bool {
			println("PANIC1")
			wg.Add(1)
			go func() {
				defer wg.Done()
				v.(MsQuicStream).shutdownClose()
			}()
			return true
		})
		mqc.streams.Clear()
		//close(mqc.acceptStreamQueue)
		//for s := range mqc.acceptStreamQueue {
		//	println("PANIC2")
		//	wg.Add(1)
		//	go func() {
		//		defer wg.Done()
		//		s.shutdownClose()
		//	}()
		//}
		wg.Wait()
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) peerClose() error {
	mqc.cancel()
	return nil
}

func (mqc MsQuicConn) appClose() error {
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	if !mqc.shutdown.Swap(true) {
		mqc.streams.Range(func(k, v any) bool {
			println("PANIC3")
			v.(MsQuicStream).abortClose()
			return true
		})
		mqc.streams.Clear()
		//close(mqc.acceptStreamQueue)
		//for s := range mqc.acceptStreamQueue {
		//	println("PANIC4")
		//	s.abortClose()
		//}
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()

	if mqc.ctx.Err() != nil {
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := cCreateStream(mqc.conn)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(stream, mqc.ctx)
	var enable C.int8_t
	if mqc.failOpenStream {
		enable = C.int8_t(1)
	} else {
		enable = C.int8_t(0)
	}
	_, loaded := mqc.streams.LoadOrStore(stream, res)
	if loaded {
		println("PANIC")
	}
	if cStartStream(stream, enable) == -1 {
		//mqc.streams.Delete(stream)
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
	return res, nil
}

func (mqc MsQuicConn) AcceptStream(ctx context.Context) (MsQuicStream, error) {

	select {
	case <-ctx.Done():
	case <-mqc.ctx.Done():
	case s, ok := <-mqc.acceptStreamQueue:
		if ok {
			return s, nil
		}
	}
	return MsQuicStream{}, fmt.Errorf("closed connection")
}

func (mqc MsQuicConn) Context() context.Context {
	return mqc.ctx
}

func (mqc MsQuicConn) RemoteAddr() net.Addr {
	return &mqc.remoteAddr
}
