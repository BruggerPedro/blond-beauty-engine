// Package cache wraps the optional Redis client. Redis is not required for
// Slice A; if ENGINE_REDIS_URL is empty, NewClient returns (nil, nil) and
// callers must handle the disabled case.
package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewClient(ctx context.Context, url string) (*redis.Client, error) {
	if url == "" {
		return nil, nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	c := redis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return c, nil
}

func Ping(c *redis.Client) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if c == nil {
			return nil // disabled
		}
		return c.Ping(ctx).Err()
	}
}
