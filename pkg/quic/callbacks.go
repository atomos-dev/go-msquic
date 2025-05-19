/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import (
	"unsafe"
)

// #include "msquic.h"
import "C"

func findConnection(c C.HQUIC) (MsQuicConn, bool) {

	res, has := connections.Load(c)
	if !has {
		return MsQuicConn{}, false
	}
	return res.(MsQuicConn), true

}

func findStream(c, s C.HQUIC) (MsQuicStream, bool) {

	conn, has := findConnection(c)
	if !has {
		return MsQuicStream{}, false // already closed
	}

	//conn.openStream.RLock()
	//defer conn.openStream.RUnlock()
	res, has := conn.streams.Load(s)
	if !has {
		return MsQuicStream{}, false // already closed
	}

	return res.(MsQuicStream), true
}

func findAndDeleteStream(c, s C.HQUIC) (MsQuicStream, bool) {

	conn, has := findConnection(c)
	if !has {
		return MsQuicStream{}, false // already closed
	}

	//conn.openStream.RLock()
	//defer conn.openStream.RUnlock()
	res, has := conn.streams.LoadAndDelete(s)
	if !has {
		return MsQuicStream{}, false // already closed
	}

	return res.(MsQuicStream), true
}

func findAndDeleteConnection(c C.HQUIC) (MsQuicConn, bool) {

	res, has := connections.LoadAndDelete(c)
	if !has {
		return MsQuicConn{}, false
	}
	return res.(MsQuicConn), true

}

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		println("Not found listener")
		cAbortConnection(c)
		return // already closed

	}
	res := newMsQuicConn(c, listener.(MsQuicListener).failOnOpenStream)

	select {
	case listener.(MsQuicListener).acceptQueue <- res:
		_, has := connections.LoadOrStore(c, res)
		if has {
			println("PANIC new conn")
		}
	default:
		println("Abort new conn")
		cAbortConnection(c)
	}
}

//export freeConnectionCallback
func freeConnectionCallback(c C.HQUIC) {
	conn, has := findAndDeleteConnection(c)
	if !has {
		println("Not found connection to free")
		cAbortConnection(c)
		return
	}
	conn.freeACK()
}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	conn, has := findConnection(c)
	if !has {
		println("Not found connection to close")
		cAbortConnection(c)
		return
	}
	conn.stopListen()
}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, buffers *C.QUIC_BUFFER, bufferCount C.uint32_t) {

	stream, has := findStream(c, s)
	if !has {
		println("New read cannot find stream")
		return
	}
	state := stream.state

	if stream.ctx.Err() == nil {
		buffersSlice := make([]C.QUIC_BUFFER, bufferCount)
		copy(buffersSlice, unsafe.Slice(buffers, bufferCount))
		go func() {
			totalLength := C.uint32_t(0)
			goBuffers := make([][]byte, len(buffersSlice))
			for i, buffer := range buffersSlice {
				if buffer.Length == 0 {
					continue
				}
				goBuffers[i] = make([]byte, buffer.Length)

				cBuffer := unsafe.Slice((*byte)(buffer.Buffer), buffer.Length)
				copy(goBuffers[i], cBuffer)
				totalLength += buffer.Length
			}
			select {
			case state.readBuffers <- goBuffers:
			default:
			}
			cStreamReceiveComplete(s, C.uint64_t(totalLength))
		}()
	} else {
		cStreamReceiveComplete(s, C.uint64_t(0))
	}
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	conn, has := findConnection(c)
	if !has {
		println("Not found connection to open stream")
		cAbortStream(s)
		return // already closed
	}
	//conn.openStream.RLock()
	//defer conn.openStream.RUnlock()
	if conn.ctx.Err() != nil || conn.listening.Load() {
		println("Not found connection to open stream 2")
		cAbortStream(s)
		return
	}

	res := newMsQuicStream(c, s, conn.ctx)

	select {
	case conn.acceptStreamQueue <- res:
		conn.streams.Store(s, res)
	default:
		println("Not space")
		cAbortStream(s)
	}
}

//export freeStreamCallback
func freeStreamCallback(c, s C.HQUIC) {
	stream, has := findAndDeleteStream(c, s)
	if !has {
		println("Not free stream")
		return
	}
	stream.freeACK()
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {
	stream, has := findStream(c, s)
	if !has {
		println("Not close stream")
		return
	}
	stream.stopWrite()
	stream.aborted()
}
