package main

import (
	"math/rand"
	"testing"
)

func TestNewClick(t *testing.T) {
	c := NewClick(rand.New(rand.NewSource(1)))
	if c.Page == "" || c.UserID == "" || c.TS == 0 {
		t.Fatal("click has empty fields")
	}
}

func TestNewClickPageIsValid(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 100; i++ {
		c := NewClick(r)
		found := false
		for _, p := range pages {
			if c.Page == p {
				found = true
			}
		}
		if !found {
			t.Fatalf("unexpected page: %s", c.Page)
		}
	}
}