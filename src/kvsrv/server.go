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

type KVServer struct {
	mu     sync.Mutex
	Map    map[string]string
	Seqmap map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.Map[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Seqmap[args.Seq]; ok {
		return
	}
	kv.Map[args.Key] = args.Value
	kv.Seqmap[args.Seq] = ""
	delete(kv.Seqmap, args.Lastseq)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if lastvalue, ok := kv.Seqmap[args.Seq]; ok {
		reply.Value = lastvalue
		return
	}
	if value, ok := kv.Map[args.Key]; ok {
		reply.Value = value
		kv.Map[args.Key] = value + args.Value
	} else {
		reply.Value = ""
		kv.Map[args.Key] = args.Value
	}
	kv.Seqmap[args.Seq] = reply.Value
	delete(kv.Seqmap, args.Lastseq)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Map = make(map[string]string)
	kv.Seqmap = make(map[int64]string)
	return kv
}
