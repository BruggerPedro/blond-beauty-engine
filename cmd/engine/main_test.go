package main

import (
	"testing"

	"github.com/blondbeauty/blond-beauty-engine/internal/config"
)

func TestValidateFakeProvidersRejectsProductionWorkers(t *testing.T) {
	cfg := &config.Config{
		Env:                   "production",
		EnablePaymentsWorkers: true,
	}
	if err := validateFakeProviders(cfg); err == nil {
		t.Fatal("expected production fake provider rejection")
	}
}

func TestValidateFakeProvidersAllowsProductionWithoutProviderWorkers(t *testing.T) {
	cfg := &config.Config{Env: "production"}
	if err := validateFakeProviders(cfg); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
}
