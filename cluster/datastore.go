package cluster

import (
	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/containous/staert"
	"github.com/docker/libkv/store"
	"golang.org/x/net/context"
	"sync"
	"time"
)

// Datastore holds a struct synced in a KV store
type Datastore struct {
	*staert.KvSource
	ctx        context.Context
	lockKey    string
	object     interface{}
	localLock  *sync.RWMutex
	remoteLock store.Locker
}

// NewDataStore creates a Datastore
func NewDataStore(kvSource *staert.KvSource, ctx context.Context, lockKey string, object interface{}) (*Datastore, error) {
	lock, err := kvSource.NewLock(lockKey, &store.LockOptions{TTL: 20 * time.Second})
	if err != nil {
		return nil, err
	}
	datastore := Datastore{
		KvSource:   kvSource,
		ctx:        ctx,
		lockKey:    lockKey,
		object:     object,
		localLock:  &sync.RWMutex{},
		remoteLock: lock,
	}
	err = datastore.watchChanges()
	if err != nil {
		return nil, err
	}
	return &datastore, nil
}

func (d *Datastore) watchChanges() error {
	stopCh := make(chan struct{})
	kvCh, err := d.Store.Watch(d.lockKey, stopCh)
	if err != nil {
		return err
	}
	go func() {
		ctx, cancel := context.WithCancel(d.ctx)
		operation := func() error {
			for {
				select {
				case <-ctx.Done():
					stopCh <- struct{}{}
					return nil
				case _, ok := <-kvCh:
					if !ok {
						cancel()
						return err
					}
					d.localLock.Lock()
					err := d.LoadConfig(d.object)
					if err != nil {
						return err
					}
					d.localLock.Unlock()
				}
			}
		}
		notify := func(err error, time time.Duration) {
			log.Errorf("KV connection error: %+v, retrying in %s", err, time)
		}
		err := backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), notify)
		if err != nil {
			log.Errorf("Cannot connect to KV server: %v", err)
		}
	}()
	return nil
}

// Put atomically stores a struct in the KV store
func (d *Datastore) Put(object interface{}) error {
	d.localLock.Lock()
	defer d.localLock.Unlock()
	stopCh := make(chan struct{})
	ctx, cancel := context.WithCancel(d.ctx)
	var errLock error
	go func() {
		_, errLock = d.remoteLock.Lock(stopCh)
		cancel()
	}()
	select {
	case <-ctx.Done():
		if errLock != nil {
			return errLock
		}
	case <-d.ctx.Done():
		stopCh <- struct{}{}
		return d.ctx.Err()
	}

	// we got the lock! Update datastore with new object.
	err := d.StoreConfig(object)
	if err != nil {
		return err
	}

	err = d.remoteLock.Unlock()
	if err != nil {
		return err
	}

	d.object = object

	return nil
}

// Get atomically get a struct from the KV store
func (d *Datastore) Get() interface{} {
	d.localLock.RLock()
	defer d.localLock.RUnlock()
	return d.object
}
