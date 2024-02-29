package dbgo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Fatal(err)
		}
		client := secretsmanager.NewFromConfig(cfg)

		crtname := strings.ReplaceAll(crt, ".crt", "")
		r, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
			SecretId: &crtname,
		})

		if err != nil {
			log.Println("Settings.load: errored trying to get secret: ", err.Error())
			return err
		}

		if err := os.WriteFile(crt, []byte(*r.SecretString), 0400); err != nil {
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

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client := secretsmanager.NewFromConfig(cfg)

	scrtnm := fmt.Sprintf("db-cxn-%s", db)
	r, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &scrtnm,
	})

	if err != nil {
		log.Printf("Db.getDbConnection: errored trying to get secret[%s]: %s\n", scrtnm, err.Error())
		return nil, err
	}

	cxn, err := d.connectPostgres(*r.SecretString)
	if err != nil {
		log.Println("Db.getDbConnection: couldnt access secret: ", err.Error())
		return nil, err
	}

	return cxn, nil
}
