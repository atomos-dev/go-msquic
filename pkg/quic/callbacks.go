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
		return // already closed
	}
	res := newMsQuicConn(c, listener.(MsQuicListener).failOnOpenStream)

	select {
	case listener.(MsQuicListener).acceptQueue <- res:
		connections.Store(c, res)
	default:
		cAbortConnection(c)
	}

}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	res, has := connections.LoadAndDelete(c)
	if !has {
		return // already closed
	}
	res.(MsQuicConn).appClose()
}

//export closePeerConnectionCallback
func closePeerConnectionCallback(c C.HQUIC) {

	res, has := connections.Load(c)

	if !has {
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
	rawConn.(MsQuicConn).openStream.Lock()
	defer rawConn.(MsQuicConn).openStream.Unlock()
	rawStream, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		return // already closed
	}

	state := rawStream.(MsQuicStream).state

	stream := rawStream.(MsQuicStream)

	buffersSlice := make([]C.QUIC_BUFFER, bufferCount)
	copy(buffersSlice, unsafe.Slice(buffers, bufferCount))
	go func() {
		totalLength := 0

		state.readBufferAccess.Lock()
		for _, buffer := range buffersSlice {
			cBuffer := unsafe.Slice((*byte)(buffer.Buffer), buffer.Length)
			n, _ := state.readBuffer.Write(cBuffer)
			totalLength += n
		}
		state.readBufferAccess.Unlock()
		cStreamReceiveComplete(s, C.uint64_t(totalLength))
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
		cAbortStream(s)
		return // already closed
	}
	conn := rawConn.(MsQuicConn)
	conn.openStream.Lock()
	defer conn.openStream.Unlock()
	if conn.ctx.Err() != nil {
		cAbortStream(s)
		return
	}

	res := newMsQuicStream(s, conn.ctx)
	select {
	case conn.acceptStreamQueue <- res:
		rawConn.(MsQuicConn).streams.Store(s, res)
	default:
		cAbortStream(s)
	}
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawConn.(MsQuicConn).openStream.Lock()
	defer rawConn.(MsQuicConn).openStream.Unlock()
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.appClose()

}

//export closePeerStreamCallback
func closePeerStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawConn.(MsQuicConn).openStream.Lock()
	defer rawConn.(MsQuicConn).openStream.Unlock()
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.appClose()

}
