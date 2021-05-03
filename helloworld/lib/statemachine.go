package lib

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type ExampleStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
}

func NewExampleStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Count:     0,
	}
}

func (e *ExampleStateMachine) Update(bytes []byte) (sm.Result, error) {
	e.Count++
	fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count %d \n", bytes, e.Count)
	return sm.Result{Value: uint64(len(bytes))}, nil
}

func (e *ExampleStateMachine) Lookup(i interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, e.Count)
	return result, nil
}

// SaveSnapshot 快照
func (e *ExampleStateMachine) SaveSnapshot(writer io.Writer, collection sm.ISnapshotFileCollection, done <-chan struct{}) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, e.Count)
	_, err := writer.Write(data)
	return err
}

// RecoverFromSnapshot 回复快照
func (e *ExampleStateMachine) RecoverFromSnapshot(reader io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	v := binary.LittleEndian.Uint64(data)
	e.Count = v
	return nil
}

// Close Close关闭IStateMachine实例
func (e *ExampleStateMachine) Close() error {
	return nil
}
