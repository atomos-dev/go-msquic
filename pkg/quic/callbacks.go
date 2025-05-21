/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import (
	"unsafe"
)

// #include "msquic.h"
import "C"

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		println("PANIC no list for new conn")
		return // already closed
	}
	res := newMsQuicConn(c, listener.(MsQuicListener).failOnOpenStream)

	_, loaded := connections.LoadOrStore(c, res)
	if loaded {
		println("PANIC already registered connection 2")
	}

	go func() {
		if res.waitStart() {
			listener.(MsQuicListener).acceptQueue <- res
		}
	}()

}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	res, has := connections.LoadAndDelete(c)
	if !has {
		println("PANIC no conn for close conn")
		return // already closed
	}
	res.(MsQuicConn).appClose()
}

//export closePeerConnectionCallback
func closePeerConnectionCallback(c C.HQUIC) {
	res, has := connections.Load(c)
	if !has {
		println("PANIC no conn for close conn")
		return // already closed
	}
	res.(MsQuicConn).peerClose()

}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, buffers *C.QUIC_BUFFER, bufferCount C.uint32_t) {

	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawStream, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		return // already closed
	}

	stream := rawStream.(MsQuicStream)
	state := stream.state

	buffersSlice := make([]C.QUIC_BUFFER, bufferCount)
	copy(buffersSlice, unsafe.Slice(buffers, bufferCount))

	go func() {
		totalLength := 0
		state.readBufferAccess.Lock()
		if stream.Context().Err() != nil {
			state.readBufferAccess.Unlock()
			return
		}
		for _, buffer := range buffersSlice {
			cBuffer := unsafe.Slice((*byte)(buffer.Buffer), buffer.Length)
			n, _ := state.readBuffer.Write(cBuffer)
			totalLength += n
			state.availBytes.Add(int64(n))
		}
		state.readBufferAccess.Unlock()
		cStreamReceiveComplete(stream.stream, C.uint64_t(totalLength))
		select {
		case stream.readSignal <- struct{}{}:
		default:
		}
	}()
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC new stream 1")
		return // already closed
	}
	conn := rawConn.(MsQuicConn)

	res := newMsQuicStream(s, conn.ctx)

	rawConn.(MsQuicConn).streams.Store(s, res)

	go func() {
		conn.acceptStreamQueue <- res
	}()
}

//export startStreamCallback
func startStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start stream")
		return // already closed
	}
	res, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		println("PANIC no stream for start stream")
		return // already closed
	}

	select {
	case res.(MsQuicStream).state.startSignal <- struct{}{}:
	default:
	}
}

//export startConnectionCallback
func startConnectionCallback(c C.HQUIC) {

	res, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start conn")
		return // already closed
	}

	select {
	case res.(MsQuicConn).startSignal <- struct{}{}:
	default:
	}
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for close stream")
		return // already closed
	}
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		//println("PANIC no stream for close stream", c, s)
		return // already closed
	}

	res.(MsQuicStream).appClose()
}

//export abortStreamCallback
func abortStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for close stream")
		return // already closed
	}
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		println("PANIC no stream for abort stream", c, s)
		return // already closed
	}

	res.(MsQuicStream).abortClose()
}
