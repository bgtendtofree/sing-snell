package snellv6

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	snell "github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type firstPacketHandler struct {
	snell.Handler
	t           *testing.T
	destination M.Socksaddr
	packets     chan []byte
}

func (h *firstPacketHandler) NewPacketConnectionEx(_ context.Context, conn N.PacketConn, _, destination M.Socksaddr, _ N.CloseHandlerFunc) {
	defer conn.Close()
	if destination != h.destination {
		h.t.Errorf("first destination = %v, want %v", destination, h.destination)
	}
	for range cap(h.packets) {
		buffer := buf.NewSize(maxPayload)
		destination, err := conn.ReadPacket(buffer)
		if err != nil {
			buffer.Release()
			h.t.Errorf("read UDP packet: %v", err)
			return
		}
		if destination != h.destination {
			h.t.Errorf("packet destination = %v, want %v", destination, h.destination)
		}
		h.packets <- bytes.Clone(buffer.Bytes())
		buffer.Release()
	}
}

// Use a TCP pipe rather than an OS UDP socket: some platforms cannot send
// datagrams this large, but Snell's record framing supports them.
func TestUDPLargeFirstPacket(t *testing.T) {
	for _, mode := range []Mode{ModeDefault, ModeUnshaped, ModeUnsafeRaw} {
		for _, size := range []int{1, 16384, 16385, 20000, 65507} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				psk := []byte("snell-first-packet-test-key")
				target := M.ParseSocksaddr("127.0.0.1:12345")
				handler := &firstPacketHandler{t: t, destination: target, packets: make(chan []byte, 2)}
				service, err := NewService(ServerOptions{PSK: psk, Mode: mode, Handler: handler})
				if err != nil {
					t.Fatal(err)
				}
				client, err := NewClient(ClientOptions{PSK: psk, Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				server, transport := net.Pipe()
				defer server.Close()
				defer transport.Close()
				deadline := time.Now().Add(5 * time.Second)
				server.SetDeadline(deadline)
				transport.SetDeadline(deadline)
				conn, err := client.DialPacketConn(transport)
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					err := service.NewConnection(context.Background(), server, M.Socksaddr{}, func(error) {})
					server.Close()
					done <- err
				}()
				payloads := [][]byte{bytes.Repeat([]byte{0x31}, size), bytes.Repeat([]byte{0x42}, 20000)}
				for _, payload := range payloads {
					if _, err := conn.WriteTo(payload, target); err != nil {
						t.Errorf("write %d bytes: %v", len(payload), err)
						transport.Close()
						break
					}
				}
				if err := <-done; err != nil {
					t.Errorf("serve UDP: %v", err)
				}
				if len(handler.packets) != len(payloads) {
					t.Fatalf("received %d packets, want %d", len(handler.packets), len(payloads))
				}
				for _, want := range payloads {
					if got := <-handler.packets; !bytes.Equal(got, want) {
						t.Errorf("UDP payload mismatch: got %d bytes, want %d", len(got), len(want))
					}
				}
			})
		}
	}
}
