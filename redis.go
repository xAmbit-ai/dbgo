package dbgo

import (
	"context"
	"fmt"
	"log"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/gomodule/redigo/redis"
)

func (d *Db) connectRedis(cxn string) (*redis.Pool, error) {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURLContext(context.Background(), cxn)
			if err != nil {
				return nil, err
			}

			return c, nil
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}

			_, err := c.Do("PING")
			return err
		},
	}
	return pool, nil
}

func (d *Db) getCacheConnection(cache string) (*redis.Pool, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Println("Db.getCacheConnection: trying to create secret client: ", err.Error())
		return nil, err
	}

	defer client.Close()

	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", d.project, fmt.Sprintf("db-cxn-%s", cache)),
	}
	r, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		log.Println("Db.getDbConnection: couldnt access secret: ", err.Error())
		return nil, err
	}

	cxn, err := d.connectRedis(string(r.Payload.Data))
	if err != nil {
		log.Println("Db.getDbConnection: cannot connect to postgresql: ", err.Error())
		return nil, err
	}

	return cxn, nil
}

func (d *Db) SetCache(key string, val []byte, ttl *int) error {
	args := []interface{}{
		key,
		val,
	}

	if ttl != nil {
		args = append(args, []interface{}{"EX", *ttl}...)
	}

	c, err := d.Cache()
	if err != nil {
		return err
	}
	conn := c.Get()
	defer conn.Close()
	if _, err := conn.Do("SET", args...); err != nil {
		return err
	}

	return nil
}

func (d *Db) GetCache(key string) ([]byte, error) {
	c, err := d.Cache()
	if err != nil {
		return nil, err
	}
	conn := c.Get()
	defer conn.Close()

	b, err := conn.Do("GET", key)
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, nil
	}

	return b.([]byte), nil
}
