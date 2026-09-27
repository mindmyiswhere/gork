package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// EmailPayload — формат payload для send_email.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func HandleSendEmail(ctx context.Context, payload []byte) ([]byte, error) {
	var p EmailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}

	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	slog.Info("email sent", "to", p.To, "subject", p.Subject)
	return []byte(`{"status":"sent"}`), nil
}

func HandleResizeImage(ctx context.Context, payload []byte) ([]byte, error) {
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if rand.Intn(100) < 30 {
		return nil, fmt.Errorf("random failure while resizing")
	}

	slog.Info("image resized")
	return []byte(`{"width":800,"height":600}`), nil
}

func HandleGenerateReport(ctx context.Context, payload []byte) ([]byte, error) {
	select {
	case <-time.After(8 * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	slog.Info("report generated")
	return []byte(`{"url":"s3://reports/abc.pdf"}`), nil
}
