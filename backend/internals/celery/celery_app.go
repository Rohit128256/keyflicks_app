package celery

import (
	"context"

	"github.com/gocelery/gocelery"
	"github.com/gomodule/redigo/redis"
)

type Celery struct {
	client *gocelery.CeleryClient
}

func NewCelery(redis_pool *redis.Pool) (*Celery, error) {

	celeryBroker := gocelery.NewRedisBroker(redis_pool)
	celeryBroker.QueueName = "video_tasks"
	celeryBackend := gocelery.NewRedisBackend(redis_pool)

	cli, err := gocelery.NewCeleryClient(
		celeryBroker,
		celeryBackend,
		0,
	)

	if err != nil {
		return nil, err
	}

	return &Celery{
		client: cli,
	}, nil
}

func (c *Celery) DispatchVideoTranscodeTask(ctx context.Context, uploadID, s3Key string) error {
	_, err := c.client.Delay("tasks.transcode_and_upload_video", uploadID, s3Key)
	return err
}
