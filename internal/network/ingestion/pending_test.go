package ingestion

import (
	"context"
	"errors"
	"testing"
)

var errBoom = errors.New("boom")

func TestStartPendingSession(t *testing.T) {
	t.Run("ready closes first", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := &Subscriber{
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				close(session.ready)
				<-ctx.Done()
				return ctx.Err()
			},
		}

		session, err := s.startPendingSession(ctx, Provider{Name: "alchemy", Url: "u"})

		if err != nil {
			t.Fatalf("startPendingSession() error = %v, want nil", err)
		}
		if session == nil {
			t.Fatal("session = nil, want non-nil")
		}
		if session.provider.Name != "alchemy" {
			t.Fatalf("provider = %q, want %q", session.provider.Name, "alchemy")
		}
	})

	t.Run("done error returned", func(t *testing.T) {
		s := &Subscriber{
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				return errBoom
			},
		}

		session, err := s.startPendingSession(context.Background(), Provider{Name: "alchemy", Url: "u"})

		if !errors.Is(err, errBoom) {
			t.Fatalf("startPendingSession() error = %v, want errBoom", err)
		}
		if session != nil {
			t.Fatal("session = non-nil, want nil")
		}
	})

	t.Run("cancelled context returns context error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s := &Subscriber{
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}

		session, err := s.startPendingSession(ctx, Provider{Name: "alchemy", Url: "u"})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("startPendingSession() error = %v, want context.Canceled", err)
		}
		if session != nil {
			t.Fatal("session = non-nil, want nil")
		}
	})
}

func TestHandoverPending(t *testing.T) {
	newFakeOld := func() (*pendingSession, *bool) {
		cancelled := false
		done := make(chan error, 1)
		old := &pendingSession{
			provider: Provider{Name: "old", Url: "u-old"},
			cancel: func() {
				cancelled = true
				done <- context.Canceled
			},
			done: done,
		}
		return old, &cancelled
	}

	t.Run("first provider succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := &Subscriber{
			providers: []Provider{{Name: "a", Url: "u-a"}, {Name: "b", Url: "u-b"}},
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				close(session.ready)
				<-ctx.Done()
				return ctx.Err()
			},
		}

		old, cancelled := newFakeOld()
		next, ok := s.handoverPending(ctx, old, Provider{Name: "b", Url: "u-b"})

		if !ok {
			t.Fatal("handoverPending() ok = false, want true")
		}
		if next == nil || next == old {
			t.Fatal("expected a new session")
		}
		if next.provider.Name != "b" {
			t.Fatalf("next provider = %q, want %q", next.provider.Name, "b")
		}
		if !*cancelled {
			t.Fatal("old session was not cancelled")
		}
	})

	t.Run("all providers fail returns old", func(t *testing.T) {
		s := &Subscriber{
			providers: []Provider{{Name: "a", Url: "u-a"}, {Name: "b", Url: "u-b"}},
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				return errBoom
			},
		}

		old, cancelled := newFakeOld()
		next, ok := s.handoverPending(context.Background(), old, Provider{Name: "a", Url: "u-a"})

		if ok {
			t.Fatal("handoverPending() ok = true, want false")
		}
		if next != old {
			t.Fatal("expected the old session to be returned")
		}
		if *cancelled {
			t.Fatal("old session should not be cancelled on failure")
		}
	})

	t.Run("cancelled context returns old", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s := &Subscriber{
			providers: []Provider{{Name: "a", Url: "u-a"}},
			connectPendingStream: func(ctx context.Context, session *pendingSession) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}

		old, cancelled := newFakeOld()
		next, ok := s.handoverPending(ctx, old, Provider{Name: "a", Url: "u-a"})

		if ok {
			t.Fatal("handoverPending() ok = true, want false")
		}
		if next != old {
			t.Fatal("expected the old session to be returned")
		}
		if *cancelled {
			t.Fatal("old session should not be cancelled on context cancel")
		}
	})
}
