package bitcask

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrObjectNotFound is the error returned when an object is not found in the collection.
	ErrObjectNotFound = errors.New("error: object not found")
)

func (b *bitcask) Collection(name string) *Collection {
	return &Collection{db: b, name: name}
}

// Collection allows you to manage a collection of objects encoded as JSON
// documents with a path-based key based on the provided name. This is convenient
// for storing complex normalized collections of objects.
type Collection struct {
	db   DB
	name string
}

func (c *Collection) makeKey(id string) Key {
	return Key(fmt.Sprintf("%s/%s", c.name, id))
}

func (c *Collection) makePrefix() Key {
	return Key(c.name)
}

// Add adds a new object to the collection
func (c *Collection) Add(id string, obj any) error {
	k := c.makeKey(id)
	v, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	return c.db.Put(k, v)
}

// Delete deletes an object from the collection
func (c *Collection) Delete(id string) error {
	return c.db.Delete(c.makeKey(id))
}

// Get returns an object from the collection
func (c *Collection) Get(id string, obj any) error {
	k := c.makeKey(id)
	data, err := c.db.Get(k)
	if err != nil {
		if err == ErrKeyNotFound {
			return ErrObjectNotFound
		}
		return err
	}

	return json.Unmarshal(data, obj)
}

// List returns a list of all objects in this collection.
func (c *Collection) List(dest any) error {
	tx := c.db.Transaction()
	defer tx.Discard()

	// Return error if dest is not a pointer to a slice.
	slice := reflect.ValueOf(dest)
	if slice.Kind() != reflect.Ptr {
		return errors.New("dest must be pointer")
	}
	slice = slice.Elem()
	if slice.Kind() != reflect.Slice {
		return errors.New("dest must be pointer to struct")
	}

	return tx.Scan(c.makePrefix(), func(k Key) error {
		data, err := tx.Get(k)
		if err != nil {
			return err
		}

		ep := reflect.New(slice.Type().Elem())

		if err := json.Unmarshal(data, ep.Interface()); err != nil {
			return err
		}

		slice.Set(reflect.Append(slice, ep.Elem()))

		return nil
	})
}

// Has returns true if an object exists by the provided id.
func (c *Collection) Has(id string) bool {
	return c.db.Has(c.makeKey(id))
}

// Count returns the number of objects in this collection.
func (c *Collection) Count() int {
	n := 0
	c.db.Scan(c.makePrefix(), func(k Key) error {
		n++
		return nil
	})
	return n
}

// Drop deletes the entire collection
func (c *Collection) Drop() error {
	tx := c.db.Transaction()
	defer tx.Discard()

	if err := tx.Scan(c.makePrefix(), func(k Key) error {
		return tx.Delete(k)
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// Exists returns true if the collection exists at all, which really just means
// whether there are any objects in the collection, so this is the same as calling
// Count() > 0.
func (c *Collection) Exists() bool {
	return c.Count() > 0
}
