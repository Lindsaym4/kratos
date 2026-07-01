package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

type User struct {
	Name string `json:"name"`
}

func TestServer(t *testing.T) {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}
	srv := NewServer()
	srv.HandleFunc("/index", fn)

	if u, err := srv.Endpoint(); err != nil || u == nil {
		t.Errorf("expected endpoint, got %v", u)
	}

	go func() {
		if err := srv.Start(context.Background()); err != nil {
			panic(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	client, err := NewClient(context.Background(), WithEndpoint(srv.lis.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// test handle
	// nolint:bodyclose
	resp, err := client.Get(context.Background(), "/index", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello" {
		t.Errorf("expected hello, got %s", string(b))
	}

	// test stop
	if err := srv.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRoute(t *testing.T) {
	srv := NewServer()
	srv.Route("/index").GET(func(w http.ResponseWriter, r *http.Request) error {
		w.Write([]byte("hello"))
		return nil
	})

	go func() {
		if err := srv.Start(context.Background()); err != nil {
			panic(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	defer srv.Stop(context.Background())

	client, err := NewClient(context.Background(), WithEndpoint(srv.lis.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// nolint:bodyclose
	resp, err := client.Get(context.Background(), "/index", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello" {
		t.Errorf("expected hello, got %s", string(b))
	}
}

func TestServer_TimeoutWithDetachedContext(t *testing.T) {
	srv := NewServer(Timeout(100 * time.Millisecond))
	srv.HandleFunc("/timeout", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(500 * time.Millisecond):
			w.Write([]byte("ok"))
		}
	})

	go func() {
		if err := srv.Start(context.Background()); err != nil && err != http.ErrServerClosed {
			t.Error(err)
		}
	}()
	defer srv.Stop(context.Background())

	time.Sleep(100 * time.Millisecond)

	u, err := srv.Endpoint()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(u.String() + "/timeout")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected status 504, got %d", resp.StatusCode)
	}
}
