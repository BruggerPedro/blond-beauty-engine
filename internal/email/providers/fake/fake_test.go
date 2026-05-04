package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blondbeauty/blond-beauty-engine/internal/email"
	"github.com/blondbeauty/blond-beauty-engine/internal/email/providers/fake"
)

func TestFakeProvider_Name(t *testing.T) {
	p := fake.New()
	if p.Name() != fake.Name {
		t.Fatalf("Name() = %q, want %q", p.Name(), fake.Name)
	}
}

func TestFakeProvider_Send_Success(t *testing.T) {
	p := fake.New()
	res, err := p.Send(context.Background(), email.Message{
		To:             "customer@example.com",
		Subject:        "Test",
		TextBody:       "Hello",
		IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res.ProviderMessageID == "" {
		t.Fatal("expected non-empty ProviderMessageID")
	}
	sent := p.Sent()
	if len(sent) != 1 {
		t.Fatalf("Sent() len = %d, want 1", len(sent))
	}
	if sent[0].To != "customer@example.com" {
		t.Errorf("Sent[0].To = %q, want %q", sent[0].To, "customer@example.com")
	}
	if sent[0].MessageID != res.ProviderMessageID {
		t.Errorf("Sent[0].MessageID = %q, want %q", sent[0].MessageID, res.ProviderMessageID)
	}
}

func TestFakeProvider_Send_PermanentFailure(t *testing.T) {
	p := fake.New()
	_, err := p.Send(context.Background(), email.Message{
		To:      "bad+fail@example.com",
		Subject: "Test",
	})
	if !errors.Is(err, email.ErrProviderPermanent) {
		t.Fatalf("expected ErrProviderPermanent, got %v", err)
	}
	if len(p.Sent()) != 0 {
		t.Fatal("permanent failure should not record a sent message")
	}
}

func TestFakeProvider_Send_TransientFailure(t *testing.T) {
	p := fake.New()
	_, err := p.Send(context.Background(), email.Message{
		To:      "retry+transient@example.com",
		Subject: "Test",
	})
	if !errors.Is(err, email.ErrProviderTransient) {
		t.Fatalf("expected ErrProviderTransient, got %v", err)
	}
	if len(p.Sent()) != 0 {
		t.Fatal("transient failure should not record a sent message")
	}
}

func TestFakeProvider_Reset(t *testing.T) {
	p := fake.New()
	_, _ = p.Send(context.Background(), email.Message{To: "a@example.com", Subject: "s"})
	_, _ = p.Send(context.Background(), email.Message{To: "b@example.com", Subject: "s"})
	if len(p.Sent()) != 2 {
		t.Fatalf("expected 2 sent messages before Reset, got %d", len(p.Sent()))
	}
	p.Reset()
	if len(p.Sent()) != 0 {
		t.Fatalf("expected 0 sent messages after Reset, got %d", len(p.Sent()))
	}
}

func TestFakeProvider_UniqueMessageIDs(t *testing.T) {
	p := fake.New()
	res1, _ := p.Send(context.Background(), email.Message{To: "a@example.com", Subject: "s"})
	res2, _ := p.Send(context.Background(), email.Message{To: "b@example.com", Subject: "s"})
	if res1.ProviderMessageID == res2.ProviderMessageID {
		t.Error("ProviderMessageIDs should be unique across sends")
	}
}
