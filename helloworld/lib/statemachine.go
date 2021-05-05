package lib

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type ExampleStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Data      []string
}

func NewExampleStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Data:      []string{},
	}
}

// update bytes输入更改的数据
func (e *ExampleStateMachine) Update(bytes []byte) (sm.Result, error) {
	e.Data = append(e.Data, fmt.Sprintf("%v", bytes))
	fmt.Printf("from ExampleStateMachine.Update(), msg: %s \n", bytes)
	return sm.Result{Value: uint64(len(bytes))}, nil
}

func (e *ExampleStateMachine) Lookup(i interface{}) (interface{}, error) {
	result, _ := json.Marshal(e.Data)
	//binary.LittleEndian.PutUint64(result, e.Count)
	fmt.Println("Lookup...")
	return result, nil
}

// SaveSnapshot 快照
func (e *ExampleStateMachine) SaveSnapshot(writer io.Writer, collection sm.ISnapshotFileCollection, done <-chan struct{}) error {
	result, _ := json.Marshal(e.Data)
	_, err := writer.Write(result)
	fmt.Println("SaveSnapshot...")
	return err
}

// RecoverFromSnapshot 回复快照
func (e *ExampleStateMachine) RecoverFromSnapshot(reader io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	fmt.Printf("RecoverFromSnapshot... : %v \n", data)
	return json.Unmarshal(data, &e.Data)
}

// Close Close关闭IStateMachine实例
func (e *ExampleStateMachine) Close() error {
	fmt.Println("Close...")
	return nil
}
