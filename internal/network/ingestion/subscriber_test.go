package ingestion

import (
	"testing"
)

func TestSubscriber_Provider(t *testing.T) {
	tests := []struct {
		name      string
		providers []Provider
		lookup    string
		want      Provider
		wantOK    bool
	}{
		{
			name:      "found with url",
			providers: []Provider{{Name: "alchemy", Url: "u1"}, {Name: "chainstack", Url: "u2"}},
			lookup:    "chainstack",
			want:      Provider{Name: "chainstack", Url: "u2"},
			wantOK:    true,
		},
		{
			name:      "empty url treated as missing",
			providers: []Provider{{Name: "alchemy", Url: ""}},
			lookup:    "alchemy",
			want:      Provider{},
			wantOK:    false,
		},
		{
			name:      "name not found",
			providers: []Provider{{Name: "alchemy", Url: "u1"}},
			lookup:    "chainstack",
			want:      Provider{},
			wantOK:    false,
		},
		{
			name:      "no providers",
			providers: nil,
			lookup:    "alchemy",
			want:      Provider{},
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscriber{providers: tt.providers}

			got, ok := s.provider(tt.lookup)

			if ok != tt.wantOK {
				t.Fatalf("provider(%q) ok = %v, want %v", tt.lookup, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("provider(%q) = %+v, want %+v", tt.lookup, got, tt.want)
			}
		})
	}
}

func TestSubscriber_AlternateProvider(t *testing.T) {
	a := Provider{Name: "alchemy", Url: "u-a"}
	b := Provider{Name: "chainstack", Url: "u-b"}
	c := Provider{Name: "third", Url: "u-c"}
	bEmpty := Provider{Name: "chainstack", Url: ""}

	tests := []struct {
		name      string
		providers []Provider
		current   string
		want      Provider
		wantOK    bool
	}{
		{name: "next provider", providers: []Provider{a, b, c}, current: "alchemy", want: b, wantOK: true},
		{name: "wrap around", providers: []Provider{a, b, c}, current: "third", want: a, wantOK: true},
		{name: "skip empty url", providers: []Provider{a, bEmpty, c}, current: "alchemy", want: c, wantOK: true},
		{name: "single provider has no alternate", providers: []Provider{a}, current: "alchemy", want: Provider{}, wantOK: false},
		{name: "unknown name starts after index 0", providers: []Provider{a, b, c}, current: "unknown", want: b, wantOK: true},
		{name: "no providers", providers: nil, current: "alchemy", want: Provider{}, wantOK: false},
		{name: "only empty alternates", providers: []Provider{a, bEmpty}, current: "alchemy", want: Provider{}, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscriber{providers: tt.providers}

			got, ok := s.alternateProvider(tt.current)

			if ok != tt.wantOK {
				t.Fatalf("alternateProvider(%q) ok = %v, want %v", tt.current, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("alternateProvider(%q) = %+v, want %+v", tt.current, got, tt.want)
			}
		})
	}
}

func TestSubscriber_NotifyPendingSwitch(t *testing.T) {
	t.Run("sends when buffer has room", func(t *testing.T) {
		s := &Subscriber{pendingSwitch: make(chan string, 4)}

		s.notifyPendingSwitch("alchemy")

		select {
		case got := <-s.pendingSwitch:
			if got != "alchemy" {
				t.Fatalf("received %q, want %q", got, "alchemy")
			}
		default:
			t.Fatal("expected notification to be delivered")
		}
	})
}
