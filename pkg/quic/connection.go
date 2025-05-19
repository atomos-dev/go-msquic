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
	abort             *atomic.Bool
	listening         *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
	failOpenStream    bool
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
		abort:             new(atomic.Bool),
		listening:         new(atomic.Bool),
		streams:           new(sync.Map),
		failOpenStream:    failOnOpen,
	}
}

func (mqc MsQuicConn) Close() error {
	mqc.cancel()
	if !mqc.shutdown.Swap(true) && !mqc.abort.Load() {
		//mqc.openStream.Lock()
		//defer mqc.openStream.Unlock()
		mqc.listening.Store(true)
		wg := sync.WaitGroup{}

		mqc.streams.Range(func(k, v any) bool {
			println("shut unclosed stream")
			wg.Add(1)
			go func() {
				defer wg.Done()
				v.(MsQuicStream).Close()
			}()
			return true
		})
		mqc.streams.Clear()
		wg.Wait()
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) freeACK() error {
	//mqc.openStream.Lock()
	//defer mqc.openStream.Unlock()
	mqc.cancel()
	mqc.listening.Store(true)
	//mqc.openStream.Lock()
	//defer mqc.openStream.Unlock()
	mqc.abort.Store(true)
	mqc.shutdown.Store(true)
	return nil
}

func (mqc MsQuicConn) stopListen() error {
	//mqc.openStream.Lock()
	//defer mqc.openStream.Unlock()
	mqc.listening.Store(true)
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	//mqc.openStream.RLock()
	//defer mqc.openStream.RLock()

	if mqc.ctx.Err() != nil || mqc.listening.Load() {
		//mqc.openStream.Unlock()
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}

	stream := cCreateStream(mqc.conn)
	if stream == nil {
		//mqc.openStream.Unlock()
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(mqc.conn, stream, mqc.ctx)
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
	//mqc.openStream.Unlock()
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
