package dbgo

import (
	"database/sql"
	"os"
	"sync"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/sync/errgroup"
)

type Db struct {
	xai     *sql.DB
	isdev   bool
	cache   *redis.Pool
	tenants map[string]*sql.DB
	project string
	lock    sync.Mutex
}

func NewDb(project string) *Db {
	isdev := true
	if os.Getenv("X_ENV") == "prod" {
		isdev = false
	}
	return &Db{
		isdev:   isdev,
		project: project,
		tenants: make(map[string]*sql.DB),
	}
}

func (d *Db) Xai() (*sql.DB, error) {
	if d.xai != nil {
		return d.xai, nil
	}

	db := "xai"
	if d.isdev {
		db = "dev-xai"
	}

	cxn, err := d.getDbConnection(db, getZoneCert())
	if err != nil {
		return nil, err
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.xai = cxn

	return d.xai, nil
}

func (d *Db) Cache() (*redis.Pool, error) {
	if d.cache != nil {
		return d.cache, nil
	}

	cache := "cache"
	if d.isdev {
		cache = "dev-cache"
	}

	cxn, err := d.getCacheConnection(cache)
	if err != nil {
		return nil, err
	}
	d.lock.Lock()
	defer d.lock.Unlock()

	d.cache = cxn
	return d.cache, nil
}

func (d *Db) Tenant(org string) (*sql.DB, error) {
	t, ok := d.tenants[org]
	if ok {
		return t, nil
	}

	cxn, err := d.getDbConnection(DBNameFromOrg(org), "roach.crt")
	if err != nil {
		return nil, err
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.tenants[org] = cxn

	return d.tenants[org], nil
}

func (d *Db) Destroy() error {
	var g errgroup.Group

	g.Go(func() error {
		if d.cache == nil {
			return nil
		}

		return d.cache.Close()
	})

	g.Go(func() error {
		if d.xai == nil {
			return nil
		}

		return d.xai.Close()
	})

	for _, v := range d.tenants {
		if v == nil {
			continue
		}

		func(toclose *sql.DB) {
			g.Go(func() error {
				return v.Close()
			})
		}(v)
	}

	return g.Wait()
}
