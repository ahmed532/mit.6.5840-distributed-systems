package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type UniqueRecord struct {
	Value     string
	timestamp int64
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]UniqueRecord
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key].Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.TimeStamp == kv.data[args.Key].timestamp {
		return
	}
	kv.data[args.Key] = UniqueRecord{Value: args.Value, timestamp: args.TimeStamp}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.TimeStamp == kv.data[args.Key].timestamp {
		return
	}
	old := kv.data[args.Key].Value
	kv.data[args.Key] = UniqueRecord{Value: old + args.Value, timestamp: args.TimeStamp}
	reply.Value = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]UniqueRecord)
	return kv
}
