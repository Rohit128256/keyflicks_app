package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	client *redis.Client
}

func NewRdisDB(goRedisClient *redis.Client) *RedisDB {
	return &RedisDB{
		client: goRedisClient,
	}
}

func (r *RedisDB) Set(ctx context.Context, key string, value interface{}, exp_time int) error {

	expiration := time.Duration(exp_time) * time.Second

	err := r.client.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisDB) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return r.client.Subscribe(ctx, channel)
}

func (r *RedisDB) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}
