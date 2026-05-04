package config

import "testing"

func TestValidateRequiresUrls(t *testing.T) {
	c := &Config{}
	if err := c.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
	c.PostgresURL = "postgres://x"
	c.RabbitURL = "amqp://x"
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ENGINE_POSTGRES_URL", "postgres://x")
	t.Setenv("ENGINE_RABBIT_URL", "amqp://x")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8081" {
		t.Fatalf("default HTTPAddr: %q", cfg.HTTPAddr)
	}
	if !cfg.EnableFakeWorker {
		t.Fatal("fake worker should default on for slice A")
	}
}
