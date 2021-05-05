package lib

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/pkg/errors"
)

const (
	appliedIndexKey    string = "disk_kv_applied_index"
	testDBDirName      string = "example-data"
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updting"
)

func syncDir(dir string) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}

	if !fileInfo.IsDir() {
		panic("not a dir")
	}

	df, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}

	defer func() {
		if cerr := df.Close(); err == nil {
			err = cerr
		}
	}()
	return df.Sync()
}

type KVData struct {
	Key string
	Val string
}

type pebbledb struct {
	mu     sync.RWMutex
	db     *pebble.DB
	ro     *pebble.IterOptions
	wo     *pebble.WriteOptions
	syncwo *pebble.WriteOptions
	closed bool
}

func (p *pebbledb) lookup(query []byte) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("db already closed")
	}

	val, closer, err := p.db.Get(query)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if len(val) == 0 {
		return []byte(""), nil
	}
	buf := make([]byte, len(val))
	copy(buf, val)
	return buf, nil
}

func (p *pebbledb) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.db != nil {
		p.db.Close()
	}
}

func createDB(dbdir string) (*pebbledb, error) {
	ro := &pebble.IterOptions{}
	wo := &pebble.WriteOptions{Sync: false}
	syncwo := &pebble.WriteOptions{Sync: true}
	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		MaxManifestFileSize: 1024 * 32,
		MemTableSize:        1024 * 32,
		Cache:               cache,
	}
	if err := os.MkdirAll(dbdir, 0755); err != nil {
		return nil, err
	}

	db, err := pebble.Open(dbdir, opts)
	if err != nil {
		return nil, err
	}
	cache.Unref()
	return &pebbledb{
		db:     db,
		ro:     ro,
		wo:     wo,
		syncwo: syncwo,
	}, nil
}

func isNewRun(dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		return true
	}

	return false
}

func getNodeDBDirName(clusterID uint64, nodeID uint64) string {
	part := fmt.Sprintf("%d_%d", clusterID, nodeID)
	return filepath.Join(testDBDirName, part)
}

func getNewRandomDBDirName(dir string) string {
	part := "%d_%d"
	rn := rand.Uint64()
	ct := time.Now().UnixNano()
	return filepath.Join(dir, fmt.Sprintf(part, rn, ct))
}

func replaceCurrentDBFile(dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := os.Rename(tmpFp, fp); err != nil {
		return err
	}
	return syncDir(dir)
}

func saveCurrentDBDirName(dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}
	fp := filepath.Join(dir, updatingDBFilename)
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
		if err := syncDir(dir); err != nil {
			panic(err)
		}
	}()
	if _, err := f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err := f.Write([]byte(dbdir)); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func getCurrentDBDirName(dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := os.OpenFile(fp, os.O_RDONLY, 0755)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		panic("corrupted content")
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err := h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		panic("corrupted content with not matched crc")
	}
	return string(content), nil
}

func createNodeDataDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func cleanupNodeDataDir(dir string) error {
	os.RemoveAll(filepath.Join(dir, updatingDBFilename))
	dbdir, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		fmt.Printf("dbdir %s, fi.name %s, dir %s\n", dbdir, fi.Name(), dir)
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != dbdir {
			fmt.Printf("removing %s\n", toDelete)
			if err := os.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}
	return nil
}

type DiskKV struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          unsafe.Pointer
	closed      bool
	aborted     bool
}

func NewDiskKV(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	d := &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
	}

	return d
}

func (d *DiskKV) queryAppliedIndex(db *pebbledb) (uint64, error) {
	val, closer, err := db.db.Get([]byte(appliedIndexKey))
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

// Open Open打开状态机并返回最后一个Raft Log条目的索引
func (d *DiskKV) Open(stopc <-chan struct{}) (uint64, error) {
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	if err := createNodeDataDir(dir); err != nil {
		panic(err)
	}
	var dbdir string
	if !isNewRun(dir) {
		if err := cleanupNodeDataDir(dir); err != nil {
			return 0, err
		}
		var err error
		dbdir, err = getCurrentDBDirName(dir)
		if err != nil {
			return 0, err
		}
		if _, err := os.Stat(dbdir); err != nil {
			if os.IsNotExist(err) {
				panic("db dir unexpectedly deleted")
			}
		}
	} else {
		dbdir = getNewRandomDBDirName(dir)
		if err := saveCurrentDBDirName(dir, dbdir); err != nil {
			return 0, err
		}
		if err := replaceCurrentDBFile(dir); err != nil {
			return 0, err
		}
	}
	db, err := createDB(dbdir)
	if err != nil {
		return 0, err
	}
	atomic.SwapPointer(&d.db, unsafe.Pointer(db))
	appliedIndex, err := d.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	d.lastApplied = appliedIndex
	return appliedIndex, nil
}

func (d *DiskKV) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if d.aborted {
		panic("update() called after abort set to true")
	}
	if d.closed {
		panic("update called after Close()")
	}
	db := (*pebbledb)(atomic.LoadPointer(&d.db))
	wb := db.db.NewBatch()
	defer wb.Close()
	for idx, e := range entries {
		fmt.Println("Update entries Idx: ", e.Index)
		dataKV := &KVData{}
		if err := json.Unmarshal(e.Cmd, dataKV); err != nil {
			panic(err)
		}
		wb.Set([]byte(dataKV.Key), []byte(dataKV.Val), db.wo)
		entries[idx].Result = sm.Result{Value: uint64(len(entries[idx].Cmd))} // 记录日志顶层
	}
	// save the applied index to the DB.
	appliedIndex := make([]byte, 8)
	binary.LittleEndian.PutUint64(appliedIndex, entries[len(entries)-1].Index)
	wb.Set([]byte(appliedIndexKey), appliedIndex, db.wo)
	if err := db.db.Apply(wb, db.syncwo); err != nil {
		return nil, err
	}
	if d.lastApplied >= entries[len(entries)-1].Index {
		panic("lastApplied not moving forward")
	}
	d.lastApplied = entries[len(entries)-1].Index
	return entries, nil
}

func (d *DiskKV) Lookup(key interface{}) (interface{}, error) {
	db := (*pebbledb)(atomic.LoadPointer(&d.db))
	if db != nil {
		v, err := db.lookup(key.([]byte))
		if err == nil && d.closed {
			panic("lookup returned valid result when DiskKV is already closed")
		}
		if err == pebble.ErrNotFound {
			return v, nil
		}
		return v, err
	}
	return nil, errors.New("db closed")
}

// Sync 同步会同步状态机的所有内核状态。自更新以来
func (d *DiskKV) Sync() error {
	return nil
}

type diskKVCtx struct {
	db       *pebbledb
	snapshot *pebble.Snapshot
}

// PrepareSnapshot 准备快照
func (d *DiskKV) PrepareSnapshot() (interface{}, error) {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}

	db := (*pebbledb)(atomic.LoadPointer(&d.db))
	return &diskKVCtx{
		db:       db,
		snapshot: db.db.NewSnapshot(),
	}, nil
}

func iteratorIsValid(iter *pebble.Iterator) bool {
	return iter.Valid()
}

// saveToWriter saves all existing key-value pairs to the provided writer.
// As an example, we use the most straight forward way to implement this.
func (d *DiskKV) saveToWriter(db *pebbledb,
	ss *pebble.Snapshot, w io.Writer) error {
	iter := ss.NewIter(db.ro)
	defer iter.Close()
	values := make([]*KVData, 0)
	for iter.First(); iteratorIsValid(iter); iter.Next() {
		kv := &KVData{
			Key: string(iter.Key()),
			Val: string(iter.Value()),
		}
		values = append(values, kv)
	}
	count := uint64(len(values))
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, count)
	if _, err := w.Write(sz); err != nil {
		return err
	}
	for _, dataKv := range values {
		data, err := json.Marshal(dataKv)
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(sz, uint64(len(data)))
		if _, err := w.Write(sz); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (d *DiskKV) SaveSnapshot(ctx interface{}, w io.Writer, i2 <-chan struct{}) error {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	ctxdata := ctx.(*diskKVCtx)
	db := ctxdata.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	ss := ctxdata.snapshot
	defer ss.Close()
	return d.saveToWriter(db, ss, w)
}

func (d *DiskKV) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	if d.closed {
		panic("recover from snapshot called after Close()")
	}
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	db, err := createDB(dbdir)
	if err != nil {
		return err
	}
	sz := make([]byte, 8)
	if _, err := io.ReadFull(reader, sz); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(sz)
	wb := db.db.NewBatch()
	defer wb.Close()
	for i := uint64(0); i < total; i++ {
		if _, err := io.ReadFull(reader, sz); err != nil {
			return err
		}
		toRead := binary.LittleEndian.Uint64(sz)
		data := make([]byte, toRead)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		dataKv := &KVData{}
		if err := json.Unmarshal(data, dataKv); err != nil {
			panic(err)
		}
		wb.Set([]byte(dataKv.Key), []byte(dataKv.Val), db.wo)
	}
	if err := db.db.Apply(wb, db.syncwo); err != nil {
		return err
	}
	if err := saveCurrentDBDirName(dir, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(dir); err != nil {
		return err
	}
	newLastApplied, err := d.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	// when d.lastApplied == newLastApplied, it probably means there were some
	// dummy entries or membership change entries as part of the new snapshot
	// that never reached the SM and thus never moved the last applied index
	// in the SM snapshot.
	if d.lastApplied > newLastApplied {
		panic("last applied not moving forward")
	}
	d.lastApplied = newLastApplied
	old := (*pebbledb)(atomic.SwapPointer(&d.db, unsafe.Pointer(db)))
	if old != nil {
		old.close()
	}
	parent := filepath.Dir(oldDirName)
	if err := os.RemoveAll(oldDirName); err != nil {
		return err
	}
	return syncDir(parent)
}

func (d *DiskKV) Close() error {
	db := (*pebbledb)(atomic.SwapPointer(&d.db, unsafe.Pointer(nil)))
	if db != nil {
		d.closed = true
		db.close()
	} else {
		if d.closed {
			panic("close called twice")
		}
	}
	return nil
}
