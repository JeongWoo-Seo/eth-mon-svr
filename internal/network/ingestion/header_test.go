package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

func TestConsumeHeaderStream_ContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	headers := make(chan *types.Header)
	errs := make(chan error)

	done := make(chan error, 1)
	go func() {
		done <- consumeHeaderStream(ctx, Provider{Name: "alchemy"}, headers, errs, time.Hour, time.Hour, func(*types.Header) {})
	}()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for context cancellation")
	}
}

func TestConsumeHeaderStream_SubscriptionClosed(t *testing.T) {
	headers := make(chan *types.Header)
	errs := make(chan error)
	close(errs)

	err := consumeHeaderStream(context.Background(), Provider{Name: "alchemy"}, headers, errs, time.Hour, time.Hour, func(*types.Header) {})

	if !errors.Is(err, errSubscriptionClosed) {
		t.Fatalf("error = %v, want errSubscriptionClosed", err)
	}
}

func TestConsumeHeaderStream_SubscriptionError(t *testing.T) {
	headers := make(chan *types.Header)
	errs := make(chan error, 1)
	errs <- errBoom

	err := consumeHeaderStream(context.Background(), Provider{Name: "alchemy"}, headers, errs, time.Hour, time.Hour, func(*types.Header) {})

	if !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want errBoom", err)
	}
}

func TestConsumeHeaderStream_PushesNonNilHeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	headers := make(chan *types.Header, 2)
	errs := make(chan error)

	pushed := make(chan *types.Header, 2)
	push := func(h *types.Header) { pushed <- h }

	done := make(chan error, 1)
	go func() {
		done <- consumeHeaderStream(ctx, Provider{Name: "alchemy"}, headers, errs, time.Hour, time.Hour, push)
	}()

	h := &types.Header{}
	headers <- nil // must be skipped
	headers <- h

	select {
	case got := <-pushed:
		if got != h {
			t.Fatalf("pushed = %v, want %v", got, h)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for header push")
	}

	// nil header must have been skipped -> exactly one push
	select {
	case extra := <-pushed:
		t.Fatalf("unexpected extra push: %v", extra)
	default:
	}

	cancel()
	<-done
}

func TestConsumeHeaderStream_WatchdogTimeout(t *testing.T) {
	headers := make(chan *types.Header)
	errs := make(chan error)

	err := consumeHeaderStream(
		context.Background(),
		Provider{Name: "alchemy"},
		headers,
		errs,
		time.Millisecond, // watchdog fires quickly
		0,                // any elapsed time exceeds the timeout
		func(*types.Header) {},
	)

	if !errors.Is(err, errHeaderTimeout) {
		t.Fatalf("error = %v, want errHeaderTimeout", err)
	}
}
