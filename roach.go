package dbgo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	_ "github.com/lib/pq"
)

func (d *Db) connectPostgres(cstr string) (*sql.DB, error) {
	// cstr := createPostgresConnectionString(user, pwd, host, db, port)
	pgconn, err := sql.Open("postgres", cstr)
	if err != nil {
		return nil, err
	}

	if err := pgconn.Ping(); err != nil {
		return nil, err
	}

	return pgconn, nil
}

func (d *Db) certIt(crt string) error {
	ctx := context.Background()
	if _, err := os.Stat(crt); errors.Is(err, os.ErrNotExist) {
		client, err := secretmanager.NewClient(ctx)
		if err != nil {
			log.Println("Db.certIt: trying to create secret client: ", err.Error())
			return err
		}
		defer client.Close()

		accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
			Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", d.project, strings.ReplaceAll(crt, ".crt", "")),
		}

		r, err := client.AccessSecretVersion(ctx, accessRequest)
		if err != nil {
			log.Println("Db.getDbConnection: couldnt access secret: ", err.Error())
			return err
		}

		if err := os.WriteFile(crt, r.Payload.Data, 0400); err != nil {
			log.Println("Db.certIt: trying to write cert file: ", err.Error())
			return err
		}
	}

	return nil
}

func (d *Db) getDbConnection(db, crt string) (*sql.DB, error) {
	if err := d.certIt(crt); err != nil {
		return nil, err
	}
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Println("Db.getDbConnection: trying to create secret client: ", err.Error())
		return nil, err
	}

	defer client.Close()
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", d.project, fmt.Sprintf("db-cxn-%s", db)),
	}

	r, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		log.Println("Db.getDbConnection: couldnt access secret: ", err.Error())
		return nil, err
	}

	cxn, err := d.connectPostgres(string(r.Payload.Data))
	if err != nil {
		log.Println("Db.getDbConnection: cannot connect to postgresql: ", err.Error())
		return nil, err
	}

	return cxn, nil
}
