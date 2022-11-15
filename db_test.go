package dbgo

import (
	"os"
	"testing"
)

func TestXai(t *testing.T) {
	db := NewDb(os.Getenv("X_PROJECT"))

	if xai, err := db.Xai(); xai == nil || err != nil {
		if err != nil {
			t.Fatal("Error connecting to xAi: ", err.Error())
		} else {
			t.Fatal("Couldnt connect with xai!")
		}
	}
}

func TestCache(t *testing.T) {
	db := NewDb(os.Getenv("X_PROJECT"))
	if cache, err := db.Cache(); cache == nil || err != nil {
		if err != nil {
			t.Fatal("Error connecting to cache: ", err.Error())
		} else {
			t.Fatal("Couldnt connect to redis")
		}
	} else {
		conn := cache.Get()
		if conn.Err() != nil {
			t.Fatal("Connection errored out: ", conn.Err().Error())
		}
	}
}

func TestCacheSetGet(t *testing.T) {
	db := NewDb(os.Getenv("X_PROJECT"))

	val := "Some awesomeness"
	if err := db.SetCache("hello-test", []byte(val), nil); err != nil {
		t.Fatal("SetCache failed: ", err.Error())
	}

	b, err := db.GetCache("hello-test")
	if err != nil {
		t.Fatal("Error getting cache: ", err.Error())
	}

	if string(b) != val {
		t.Fail()
	}
}
