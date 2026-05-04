package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blondbeauty/blond-beauty-engine/internal/fiscal"
	"github.com/blondbeauty/blond-beauty-engine/internal/fiscal/providers/fake"
)

func baseReq(totalCents int64) fiscal.IssueRequest {
	return fiscal.IssueRequest{
		InvoiceID:      "inv-001",
		OrderID:        "order-001",
		PaymentID:      "pay-001",
		Model:          fiscal.ModelNFe,
		Series:         "1",
		IdempotencyKey: "idem-001",
		TotalCents:     totalCents,
		Currency:       "BRL",
	}
}

func TestFakeProvider_Name(t *testing.T) {
	p := fake.New()
	if p.Name() != fake.Name {
		t.Fatalf("Name() = %q, want %q", p.Name(), fake.Name)
	}
}

func TestFakeProvider_Issue_Authorized(t *testing.T) {
	p := fake.New()
	res, rej, err := p.IssueInvoice(context.Background(), baseReq(5000))
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if res.AccessKey == "" {
		t.Error("expected non-empty AccessKey")
	}
	if res.Protocol == "" {
		t.Error("expected non-empty Protocol")
	}
	if res.Number == "" {
		t.Error("expected non-empty Number")
	}
	if res.XMLStorageKey == "" || res.DANFEStorageKey == "" {
		t.Error("expected non-empty storage keys")
	}
	if rej.Code != "" {
		t.Errorf("expected empty rejection on success, got %q", rej.Code)
	}

	issued := p.Issued()
	if len(issued) != 1 {
		t.Fatalf("Issued() len = %d, want 1", len(issued))
	}
	if issued[0].AccessKey != res.AccessKey {
		t.Errorf("recorded AccessKey mismatch")
	}
}

func TestFakeProvider_Issue_Rejected(t *testing.T) {
	p := fake.New()
	_, rej, err := p.IssueInvoice(context.Background(), baseReq(1013))
	if !errors.Is(err, fiscal.ErrProviderRejected) {
		t.Fatalf("expected ErrProviderRejected, got %v", err)
	}
	if rej.Code == "" {
		t.Error("expected non-empty rejection code")
	}
	if len(p.Issued()) != 0 {
		t.Error("rejection must not record an issued entry")
	}
}

func TestFakeProvider_Issue_Transient(t *testing.T) {
	p := fake.New()
	_, _, err := p.IssueInvoice(context.Background(), baseReq(1017))
	if !errors.Is(err, fiscal.ErrProviderTransient) {
		t.Fatalf("expected ErrProviderTransient, got %v", err)
	}
}

func TestFakeProvider_Cancel_Success(t *testing.T) {
	p := fake.New()
	res, err := p.CancelInvoice(context.Background(), fiscal.CancelRequest{
		InvoiceID:      "inv-001",
		AccessKey:      "FAKEKEY",
		IdempotencyKey: "idem-cancel",
		Reason:         "customer requested cancellation",
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if res.Protocol == "" {
		t.Error("expected non-empty Protocol")
	}
}

func TestFakeProvider_Cancel_Transient(t *testing.T) {
	p := fake.New()
	_, err := p.CancelInvoice(context.Background(), fiscal.CancelRequest{
		InvoiceID: "inv+transient",
		AccessKey: "FAKEKEY",
	})
	if !errors.Is(err, fiscal.ErrProviderTransient) {
		t.Fatalf("expected ErrProviderTransient, got %v", err)
	}
}

func TestFakeProvider_Reset(t *testing.T) {
	p := fake.New()
	_, _, _ = p.IssueInvoice(context.Background(), baseReq(5000))
	if len(p.Issued()) != 1 {
		t.Fatalf("expected 1 issued, got %d", len(p.Issued()))
	}
	p.Reset()
	if len(p.Issued()) != 0 {
		t.Fatal("expected 0 after Reset")
	}
}

func TestFakeProvider_AccessKey_Is44Chars(t *testing.T) {
	p := fake.New()
	res, _, err := p.IssueInvoice(context.Background(), baseReq(5000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.AccessKey) != 44 {
		t.Errorf("AccessKey length = %d, want 44", len(res.AccessKey))
	}
}
