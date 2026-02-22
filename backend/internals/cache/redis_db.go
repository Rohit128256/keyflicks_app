package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	Client *redis.Client
}

func NewRdisDB(goRedisClient *redis.Client) *RedisDB {
	return &RedisDB{
		Client: goRedisClient,
	}
}

// set a value in redis with expiry time
func (r *RedisDB) Set(ctx context.Context, key string, value any, exp_time int) error {

	expiration := time.Duration(exp_time) * time.Second

	err := r.Client.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return err
	}
	return nil
}

// subscribe to a redis channel
func (r *RedisDB) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return r.Client.Subscribe(ctx, channel)
}

// get a value from redis
func (r *RedisDB) Get(ctx context.Context, key string) (string, error) {
	return r.Client.Get(ctx, key).Result()
}

// Add a value to a particular redis set (It's more of a like map with Sets as Values)
func (r *RedisDB) AddToSet(ctx context.Context, key string, values ...any) error {
	err := r.Client.SAdd(ctx, key, values).Err()

	if err != nil {
		return err
	}

	return nil
}

// Pop values in batch from particular redis set
func (r *RedisDB) PopFromSet(ctx context.Context, key string, count int64) ([]string, error) {
	return r.Client.SPopN(ctx, key, count).Result()
}

// this data structure of redis (Hashset) is like a nested haspmap
func (r *RedisDB) IncrementHashField(ctx context.Context, key string, field string, incr int64) (int64, error) {
	return r.Client.HIncrBy(ctx, key, field, incr).Result()
}

// remove anything using it's key from redis
func (r *RedisDB) Remove(ctx context.Context, key string) error {
	return r.Client.Del(ctx, key).Err()
}

// set expiry time for anything using it's key
func (r *RedisDB) Expire(ctx context.Context, key string, exp_time int) error {
	expiration := time.Duration(exp_time) * time.Minute
	err := r.Client.Expire(ctx, key, expiration).Err()
	if err != nil {
		return err
	}
	return nil
}

// Returns a Redis Pipeliner to batch commands and reduce network trips
func (r *RedisDB) Pipeline() redis.Pipeliner {
	return r.Client.Pipeline()
}
