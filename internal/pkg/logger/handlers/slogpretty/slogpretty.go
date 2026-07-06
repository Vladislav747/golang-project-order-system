package slogpretty

import (
	"context"
	"encoding/json"
	"io"
	stdLog "log"
	"log/slog"

	"github.com/fatih/color"
)

type PrettyHandlerOptions struct {
	// SlogOpts пробрасывает стандартные настройки slog.HandlerOptions
	// (например, уровень логирования, AddSource и т.д.).
	SlogOpts *slog.HandlerOptions
}

type PrettyHandler struct {
	// Handler оставлен для совместимости с базовым интерфейсом slog.Handler.
	slog.Handler
	// l печатает уже подготовленную "красивую" строку лога.
	l *stdLog.Logger
	// attrs содержит дополнительные атрибуты из WithAttrs().
	attrs []slog.Attr
}

func (opts PrettyHandlerOptions) NewPrettyHandler(
	out io.Writer,
) *PrettyHandler {
	// Используем JSONHandler как базовый встроенный хендлер, чтобы сохранить
	// совместимость по поведению с slog, а вывод для человека формируем в Handle().
	h := &PrettyHandler{
		Handler: slog.NewJSONHandler(out, opts.SlogOpts),
		l:       stdLog.New(out, "", 0),
	}

	return h
}

func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	// Подготавливаем строку уровня логирования и раскрашиваем её.
	level := r.Level.String() + ":"

	switch r.Level {
	case slog.LevelDebug:
		level = color.MagentaString(level)
	case slog.LevelInfo:
		level = color.BlueString(level)
	case slog.LevelWarn:
		level = color.YellowString(level)
	case slog.LevelError:
		level = color.RedString(level)
	}

	fields := make(map[string]interface{}, r.NumAttrs())

	// Собираем атрибуты текущей записи.
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()

		return true
	})

	// Добавляем "наследованные" атрибуты, переданные через WithAttrs().
	for _, a := range h.attrs {
		fields[a.Key] = a.Value.Any()
	}

	var b []byte
	var err error

	if len(fields) > 0 {
		// Форматируем поля в многострочный JSON, чтобы лог было легче читать в консоли.
		b, err = json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return err
		}
	}

	// Формируем итоговую строку: время, уровень, сообщение и JSON-поля.
	timeStr := r.Time.Format("[15:05:05.000]")
	msg := color.CyanString(r.Message)

	h.l.Println(
		timeStr,
		level,
		msg,
		color.WhiteString(string(b)),
	)

	return nil
}

func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Возвращаем новый handler с тем же writer/logger, но с новым набором attrs.
	// Это соответствует иммутабельной модели slog.Handler.
	return &PrettyHandler{
		Handler: h.Handler,
		l:       h.l,
		attrs:   attrs,
	}
}

func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	// Группировка проксируется в базовый хендлер.
	// Локально группы не раскрываются, так как pretty-вывод формируется вручную в Handle().
	// TODO: implement
	return &PrettyHandler{
		Handler: h.Handler.WithGroup(name),
		l:       h.l,
	}
}
