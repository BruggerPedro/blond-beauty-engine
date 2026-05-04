package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/blondbeauty/blond-beauty-engine/internal/payments"
)

func mkRef(amount int64) payments.PaymentRef {
	return payments.PaymentRef{
		PaymentID:      "p1",
		OrderID:        "o1",
		IdempotencyKey: "idem-1",
		Amount:         payments.Money{AmountCents: amount, Currency: "BRL"},
	}
}

func TestCreateAuthorized(t *testing.T) {
	p := New()
	res, err := p.CreatePayment(context.Background(), payments.CreateRequest{Ref: mkRef(2500)})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if res.Status != payments.StatusAuthorized {
		t.Fatalf("status: %v", res.Status)
	}
	if res.ProviderPaymentID == "" {
		t.Fatal("provider id empty")
	}
}

func TestCreateRejected(t *testing.T) {
	p := New()
	_, err := p.CreatePayment(context.Background(), payments.CreateRequest{Ref: mkRef(1013)})
	if !errors.Is(err, payments.ErrProviderRejected) {
		t.Fatalf("want rejected, got %v", err)
	}
}

func TestCreateTransient(t *testing.T) {
	p := New()
	_, err := p.CreatePayment(context.Background(), payments.CreateRequest{Ref: mkRef(1017)})
	if !errors.Is(err, payments.ErrProviderTransient) {
		t.Fatalf("want transient, got %v", err)
	}
}

func TestCreateRequiresAction(t *testing.T) {
	p := New()
	res, err := p.CreatePayment(context.Background(), payments.CreateRequest{Ref: mkRef(1023)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != payments.StatusRequiresAction {
		t.Fatalf("status: %v", res.Status)
	}
	if res.NextAction == nil || res.NextAction.URL == "" {
		t.Fatal("next action missing")
	}
}

func TestCaptureRefundLifecycle(t *testing.T) {
	p := New()
	ref := mkRef(2500)
	cr, err := p.CreatePayment(context.Background(), payments.CreateRequest{Ref: ref})
	if err != nil {
		t.Fatal(err)
	}
	ref.ProviderPaymentID = cr.ProviderPaymentID

	cap, err := p.CapturePayment(context.Background(), payments.CaptureRequest{Ref: ref, Amount: ref.Amount})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if cap.Status != payments.StatusPaid {
		t.Fatalf("capture status: %v", cap.Status)
	}

	rf, err := p.RefundPayment(context.Background(), payments.RefundRequest{Ref: ref, Amount: ref.Amount})
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	if rf.Status != payments.StatusRefunded {
		t.Fatalf("refund status: %v", rf.Status)
	}
}

func TestIllegalTransition(t *testing.T) {
	p := New()
	_, err := p.CapturePayment(context.Background(), payments.CaptureRequest{Ref: mkRef(2500)})
	if !errors.Is(err, payments.ErrIllegalTransition) {
		t.Fatalf("want illegal, got %v", err)
	}
}

func TestVerifyWebhook(t *testing.T) {
	p := New()
	body := []byte(`{"event_id":"evt_1","provider_payment_id":"fake_1","status":"paid"}`)
	headers := map[string]string{sigHeader: SignBody(body)}
	if err := p.VerifyWebhook(context.Background(), headers, body); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	headers[sigHeader] = SignBody([]byte(`different body`))
	err := p.VerifyWebhook(context.Background(), headers, body)
	if !errors.Is(err, payments.ErrWebhookSignatureInvalid) {
		t.Fatalf("want invalid signature, got %v", err)
	}
}

func TestVerifyWebhookRejectsMalformedSignature(t *testing.T) {
	p := New()
	err := p.VerifyWebhook(context.Background(), map[string]string{sigHeader: "not-hex"}, []byte("{}"))
	if !errors.Is(err, payments.ErrWebhookSignatureInvalid) {
		t.Fatalf("want invalid signature, got %v", err)
	}
}
