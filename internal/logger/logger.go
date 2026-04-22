package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"
)

type Config struct {
	Service string
	Env     string
	Level   slog.Level
	Output  io.Writer
}

type ctxKeyType int

const loggerKey ctxKeyType = iota

func New(cfg Config) *slog.Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stdout //콘솔 출력
	}
	opts := &slog.HandlerOptions{Level: cfg.Level} //출력 레벨 설정
	var h slog.Handler
	if cfg.Env == "prod" { //출력 방법과 옵션 설정을 핸들러에 포함
		h = slog.NewJSONHandler(out, opts)
	} else {
		h = slog.NewTextHandler(out, opts)
	}

	l := slog.New(h).With("service", cfg.Service, "env", cfg.Env)
	slog.SetDefault(l) // 시스템 기본 로거로 설정
	return l
}

func WithContext(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}
func FromContext(ctx context.Context) *slog.Logger {
	if v, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return v
	}
	return slog.Default()
}

func WithTrace(ctx context.Context, traceID string) context.Context {
	return WithContext(ctx, FromContext(ctx).With("trace_id", traceID))
}

func Debug(ctx context.Context, msg string, args ...any) { FromContext(ctx).Debug(msg, args...) }
func Info(ctx context.Context, msg string, args ...any)  { FromContext(ctx).Info(msg, args...) }
func Warn(ctx context.Context, msg string, args ...any)  { FromContext(ctx).Warn(msg, args...) }
func Error(ctx context.Context, msg string, err error, args ...any) {
	log := FromContext(ctx)

	if err == nil {
		log.Error(msg, args...)
		return
	}

	finalArgs := append([]any{"error", err}, args...)
	log.Error(msg, finalArgs...)
}

func Duration(start time.Time) any { return slog.Duration("elapsed", time.Since(start)) }
