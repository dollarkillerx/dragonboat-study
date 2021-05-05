package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"testing"
)

type Cpx struct {
	CName string `json:"c_name"`
	Age   int    `json:"age"`
}

func TestDisk(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	writer := bufio.NewWriter(buffer)

	cps := make([]Cpx, 0)
	for i := 0; i < 10000; i++ {
		cps = append(cps, Cpx{
			CName: fmt.Sprintf("h_%d", i),
			Age:   i,
		})
	}

	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, uint64(len(cps))) // 写入总条数
	_, err := writer.Write(sz)
	if err != nil {
		log.Fatalln(err)
	}

	for _, v := range cps {
		data, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}

		binary.LittleEndian.PutUint64(sz, uint64(len(data))) // 写入长度
		_, err = writer.Write(sz)
		if err != nil {
			log.Fatalln(err)
		}
		_, err = writer.Write(data) // 写入文件
		if err != nil {
			log.Fatalln(err)
		}
	}
	err = writer.Flush()
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(buffer.String())

	// encoding
	sz = make([]byte, 8)
	_, err = io.ReadFull(buffer, sz)
	if err != nil {
		log.Fatalln(err)
	}

	total := binary.LittleEndian.Uint64(sz) // 读取总条数
	for i := uint64(0); i < total; i++ {
		_, err := io.ReadFull(buffer, sz)
		if err != nil {
			log.Fatalln(err)
		}
		toRead := binary.LittleEndian.Uint64(sz) // 读取buf长度
		data := make([]byte, toRead)
		_, err = io.ReadFull(buffer, data) // 读取buf
		if err != nil {
			log.Fatalln(err)
		}

		rc := Cpx{}
		err = json.Unmarshal(data, &rc)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(string(data))
	}
}
