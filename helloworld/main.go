package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/dollarkillerx/dragonboat-study/helloworld/lib"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/goutils/syncutil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/pkg/errors"
)

const (
	exampleClusterID uint64 = 128 // 族ID
)

var (
	// default raft cluster addr
	addresses = []string{
		"127.0.0.1:63001",
		"127.0.0.1:63002",
		"127.0.0.1:63003",
	}

	errNotMembershipChange = errors.New("not a membership change request")
)

// 发出成员更改请求
func makeMembershipChange(nh *dragonboat.NodeHost, cmd string,
	addr string, nodeID uint64,
) {
	var rs *dragonboat.RequestState
	var err error

	switch cmd {
	case "add":
		rs, err = nh.RequestAddNode(exampleClusterID, nodeID, addr, 0, 3*time.Second)
	case "remove":
		rs, err = nh.RequestDeleteNode(exampleClusterID, nodeID, 0, 3*time.Second)
	default:
		panic("unknown cmd")
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "membership change failed, %v\n", err)
		return
	}

	select {
	case r := <-rs.ResultC():
		if r.Completed() {
			fmt.Fprintf(os.Stdout, "membership change completed successfully\n")
		} else {
			fmt.Fprintf(os.Stderr, "membership change failed\n")
		}
	}
}

// splitMembershipChangeCmd 尝试将输入str解析为成员资格更改
func splitMembershipChangeCmd(v string) (string, string, uint64, error) {
	parts := strings.Split(v, " ")
	if len(parts) == 2 || len(parts) == 3 {
		cmd := strings.ToLower(strings.TrimSpace(parts[0]))
		if cmd != "add" && cmd != "remove" {
			return "", "", 0, errNotMembershipChange
		}

		addr := ""
		var nodeIDStr string
		var nodeID uint64
		var err error
		if cmd == "add" {
			addr = strings.TrimSpace(parts[1])
			nodeIDStr = strings.TrimSpace(parts[2])
		} else {
			nodeIDStr = strings.TrimSpace(parts[1])
		}
		if nodeID, err = strconv.ParseUint(nodeIDStr, 10, 64); err != nil {
			return "", "", 0, errNotMembershipChange
		}
		return cmd, addr, nodeID, nil
	}

	return "", "", 0, errNotMembershipChange
}

func main() {
	nodeID := flag.Int("nodeid", 1, "NodeID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	flag.Parse()
	if len(*addr) == 0 && *nodeID != 1 && *nodeID != 2 && *nodeID != 3 {
		fmt.Fprintf(os.Stderr, "node id must be 1, 2 or 3 when address is not specified\n")
		os.Exit(1)
	}

	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
	initialMembers := make(map[uint64]string)

	if !*join {
		for idx, v := range addresses {
			initialMembers[uint64(idx+1)] = v
		}
	}
	var nodeAddr string
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else {
		nodeAddr = initialMembers[uint64((*nodeID))]
	}

	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// 更改日志详细程度
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	// config for raft node
	rc := config.Config{
		NodeID:             uint64(*nodeID),
		ClusterID:          exampleClusterID,
		ElectionRTT:        10, // 选举时间 毫秒
		HeartbeatRTT:       1,  // 心跳时间
		CheckQuorum:        true,
		SnapshotEntries:    10, // 快照时间
		CompactionOverhead: 5,
	}

	datadir := filepath.Join("example-data", "helloworld-data", fmt.Sprintf("node%d", *nodeID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,  // 内容的存储位置。
		NodeHostDir:    datadir,  // NodeHostDir是其他所有内容的存储位置。
		RTTMillisecond: 200,      // RTTMillisecond是NodeHost之间的平均往返时间
		RaftAddress:    nodeAddr, // RaftAddress用于标识NodeHost实例
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}

	if err := nh.StartCluster(initialMembers, *join, lib.NewExampleStateMachine, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
	}

	raftStopper := syncutil.NewStopper()
	consoleStopper := syncutil.NewStopper()
	ch := make(chan string, 16)
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				// no data will be lost/corrupted if nodehost.Stop() is not called
				nh.Stop()
				return
			}
			ch <- s
		}
	})

	raftStopper.RunWorker(func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				result, err := nh.SyncRead(ctx, exampleClusterID, []byte{})
				cancel()
				if err == nil {
					var count uint64
					count = binary.LittleEndian.Uint64(result.([]byte))
					fmt.Fprintf(os.Stdout, "count: %d\n", count)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})

	raftStopper.RunWorker(func() {
		cs := nh.GetNoOPSession(exampleClusterID)
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}

				msg := strings.Replace(v, "\n", "", 1)
				if cmd, addr, nodeID, err := splitMembershipChangeCmd(msg); err != nil {
					makeMembershipChange(nh, cmd, addr, nodeID)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, err := nh.SyncPropose(ctx, cs, []byte(msg))
					cancel()
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
					}
				}
			}
		}
	})

	raftStopper.Wait()
}
