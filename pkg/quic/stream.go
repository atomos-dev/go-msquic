package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "msquic.h"
import "C"

type Stream interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	WriteTo(w io.Writer) (n int64, err error)
	ReadFrom(r io.Reader) (n int64, err error)
	Close() error
	SetDeadline(ttl time.Time) error
	SetReadDeadline(ttl time.Time) error
	SetWriteDeadline(ttl time.Time) error
	Context() context.Context
}

type streamState struct {
	shutdown atomic.Bool
	abort    atomic.Bool
	//readBufferAccess sync.Mutex
	//readBuffer       bytes.Buffer
	readDeadline  time.Time
	writeDeadline time.Time
	writeAccess   sync.Mutex
	readBuffers   chan [][]byte
	leftover      [][]byte
	leftoverIndex int
}

//func (ss *streamState) hasReadData() bool {
//	ss.readBufferAccess.Lock()
//	defer ss.readBufferAccess.Unlock()
//	return ss.readBuffer.Len() != 0
//}

type MsQuicStream struct {
	connection C.HQUIC
	stream     C.HQUIC
	ctx        context.Context
	cancel     context.CancelFunc
	state      *streamState
	peerSignal chan struct{}
}

func newMsQuicStream(c, s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		connection: c,
		stream:     s,
		ctx:        ctx,
		cancel:     cancel,
		state: &streamState{
			//readBuffer:    bytes.Buffer{},
			readDeadline:  time.Time{},
			writeDeadline: time.Time{},
			shutdown:      atomic.Bool{},
			abort:         atomic.Bool{},
			readBuffers:   make(chan [][]byte, 20),
		},
		peerSignal: make(chan struct{}, 1),
	}
	return res
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	state := mqs.state
	if mqs.ctx.Err() != nil {
		return 0, io.EOF
	}

	deadline := state.readDeadline
	ctx := mqs.ctx
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	n := 0

	if len(state.leftover) != 0 {
		for ; state.leftoverIndex < len(state.leftover); state.leftoverIndex++ {
			nn := copy(data, state.leftover[state.leftoverIndex])
			n += nn
			if nn < len(state.leftover[state.leftoverIndex]) {
				state.leftover[state.leftoverIndex] = state.leftover[state.leftoverIndex][nn:]
				return n, nil
			}
		}
		state.leftover = nil
	} else {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return n, os.ErrDeadlineExceeded
			}
			return n, io.EOF
		case buf := <-state.readBuffers:
			var subbuf []byte
			for state.leftoverIndex, subbuf = range buf {
				nn := copy(data[n:], subbuf)
				n += nn
				if nn < len(subbuf) {
					state.leftover = buf
					state.leftover[state.leftoverIndex] = state.leftover[state.leftoverIndex][nn:]
					return n, nil
				}
			}
		}
	}
loop:
	for n < len(data) {
		select {
		case buf := <-state.readBuffers:
			var subbuf []byte
			for state.leftoverIndex, subbuf = range buf {
				nn := copy(data[n:], subbuf)
				n += nn
				if nn < len(subbuf) {
					state.leftover = buf
					state.leftover[state.leftoverIndex] = state.leftover[state.leftoverIndex][nn:]
					return n, nil
				}
			}
		default:
			break loop
		}
	}

	return n, nil
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	state := mqs.state
	ctx := mqs.ctx
	if ctx.Err() != nil {
		return 0, io.EOF
	}
	deadline := state.writeDeadline
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	offset := 0
	size := len(data)
	state.writeAccess.Lock()
	for offset != len(data) && ctx.Err() == nil {
		n := cStreamWrite(mqs.connection, mqs.stream, (*C.uint8_t)(unsafe.SliceData(data[offset:])), C.int64_t(size))
		if n == -1 {
			return int(offset), fmt.Errorf("write stream error %v/%v", offset, size)
		}
		offset += int(n)
		size -= int(n)
	}
	state.writeAccess.Unlock()
	runtime.KeepAlive(data)
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return int(offset), os.ErrDeadlineExceeded
		}
		return int(offset), io.ErrUnexpectedEOF
	}
	return len(data), nil
}

func (mqs MsQuicStream) SetDeadline(ttl time.Time) error {
	err := mqs.SetReadDeadline(ttl)
	err2 := mqs.SetWriteDeadline(ttl)
	return errors.Join(err, err2)
}

func (mqs MsQuicStream) SetReadDeadline(ttl time.Time) error {
	mqs.state.readDeadline = ttl
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	mqs.state.writeDeadline = ttl
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx

}

func (mqs MsQuicStream) WriteTo(w io.Writer) (n int64, err error) {
	state := mqs.state
	deadline := state.readDeadline
	ctx := mqs.ctx
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	for ; state.leftoverIndex < len(mqs.state.leftover); state.leftoverIndex++ {
		for {
			nn, err := w.Write(mqs.state.leftover[state.leftoverIndex][:])
			n += int64(nn)
			mqs.state.leftover[state.leftoverIndex] = mqs.state.leftover[state.leftoverIndex][nn:]
			if err != nil {
				return n, err
			}
			if len(mqs.state.leftover[state.leftoverIndex]) == 0 {
				break
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return n, os.ErrDeadlineExceeded
			}
			return n, io.EOF
		case buf := <-state.readBuffers:
			for _, subbuf := range buf {
				//nn := 0
				//for len(subbuf[nn:]) > 0 {
				nn, err := w.Write(subbuf)
				n += int64(nn)
				if err != nil {
					return n, err
				}
				//}
			}
		}
	}
}

func (mqs MsQuicStream) ReadFrom(r io.Reader) (n int64, err error) {
	var buffer [32 * 1024]byte
	for mqs.ctx.Err() == nil {
		bn, err := r.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = mqs.Write(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			return n, err
		}
	}
	return n, io.EOF
}

func (mqs MsQuicStream) freeACK() {
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	mqs.state.abort.Store(true)
}

func (mqs MsQuicStream) stopWrite() {
	mqs.cancel()
	//mqs.state.writeAccess.Lock()
	//defer mqs.state.writeAccess.Unlock()
}

func (mqs MsQuicStream) aborted() {
	mqs.state.abort.Store(true)
}

func (mqs MsQuicStream) peerCloseACK() {
	select {
	case mqs.peerSignal <- struct{}{}:
	default:
	}
}

// Close is a definitive operation
// The stream cannot be receive anything after that call
func (mqs MsQuicStream) Close() error {
	if !mqs.state.shutdown.Swap(true) && !mqs.state.abort.Load() {
		mqs.cancel()
		//mqs.state.writeAccess.Lock()
		cShutdownStream(mqs.stream)
		//mqs.state.writeAccess.Unlock()
		select {
		case <-mqs.peerSignal:
		case <-time.After(3 * time.Second):
			//mqs.state.writeAccess.Lock()
			//defer mqs.state.writeAccess.Unlock()
			if !mqs.state.abort.Load() {
				cAbortStream(mqs.stream)
			}
		}
	}
	return nil
}
