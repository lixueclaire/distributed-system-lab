package diskv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Type       string
    Key        string
    Value      string
    Id         string
    Client     string
    Config     shardmaster.Config //for reconfiguration
    NewData    TranDataReply //for reconfiguration
    Mark       int64
}


type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	now        int
    data       map[string] string
    last       map[string] string
    config     shardmaster.Config
}

func (kv *DisKV) Wait(seq int) Op {
    to := 10 * time.Millisecond
    for {
        status, value := kv.px.Status(seq)
        if status == paxos.Decided{
            return value.(Op)
        }
        time.Sleep(to)
        if to < 10 * time.Second {
            to *= 2
        }
    }
}

func (kv *DisKV) AddLog(op Op){
    for {
        //must check every time
        if op.Type == "Reconfiguration" {
            if op.Config.Num <= kv.config.Num {
                return
            }
        } else if op.Type == "TranData" {
        } else {
            t, ok := kv.last[op.Client + op.Type]
            if (ok && t >= op.Id) || kv.config.Shards[key2shard(op.Key)] != kv.gid{
                return
            }
        }
        seq := kv.now + 1
        kv.px.Start(seq, op)
        res := kv.Wait(seq)
        if res.Type == "Put" {
			//fmt.Println("Put", kv.gid, kv.me, res.Key, op.Key, res.Mark, op.Mark);
			//fmt.Println("Put", kv.gid, kv.me, res.Key, res.Value)
            kv.data[res.Key] = res.Value
            kv.last[res.Client + res.Type] = res.Id
        } else if res.Type == "Append" {
            //fmt.Println("Append", kv.gid, kv.me, res.Key, res.Value, kv.data[res.Key]+res.Value)
			kv.data[res.Key] += res.Value
            kv.last[res.Client + res.Type] = res.Id
        } else if res.Type == "Get" {
			//fmt.Println("Get", kv.gid, kv.me, res.Key, kv.data[res.Key], kv.config.Num)
            kv.last[res.Client + res.Type] = res.Id
        } else if res.Type == "Reconfiguration" {
            //fmt.Println("------", kv.gid, kv.me, kv.config.Num, res.Config.Num)
            for key, value := range res.NewData.Data {
                //fmt.Println(kv.config.Shards[key2shard(key)], res.Config.Shards[key2shard(key)], key, "old", kv.data[key], "new", value)
                kv.data[key] = value
            }
            for key, value := range res.NewData.Last {
                t, ok := kv.last[key]
                if !(ok && t >= value) {
                    kv.last[key] = value
                }
            }
            kv.config = res.Config
        } else {
			//TranData
        }
        kv.px.Done(seq)
        kv.now ++
        if res.Mark == op.Mark {
            return
        }
    }
}


//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}


func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if args.Num > kv.config.Num {
        reply.Err = ErrBehindConfig
        return nil
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: "Get", Key: args.Key, Id: args.Id, Client: args.Me, Mark: rand.Int63() }
    kv.AddLog(op)
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
        reply.Err = ErrWrongGroup
        return nil
    }
    tmp, ok := kv.data[args.Key]
    if !ok {
        reply.Err = ErrNoKey
    } else {
        reply.Value = tmp
    }
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	if args.Num > kv.config.Num {
        reply.Err = ErrBehindConfig
        return nil
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Client: args.Me, Mark: rand.Int63() + int64(kv.me)}
	//fmt.Println("PP", args.Key, args.Value);
    kv.AddLog(op)
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
        reply.Err = ErrWrongGroup
        return nil
    }
	return nil
}

func (kv *DisKV) TranData(args *TranDataArgs, reply *TranDataReply) error {
    if args.Num > kv.config.Num {
        reply.Err = ErrBehindConfig
        return nil
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: "TranData", Mark: rand.Int63()}
    kv.AddLog(op) //it's necessary
    if args.Num > kv.config.Num {
        reply.Err = ErrBehindConfig
        return nil
    }
    shard := args.Shard
    reply.Data = map[string] string{}
    reply.Last = map[string] string{}
    for key, value := range kv.data {
        if key2shard(key) == shard {
            reply.Data[key] = value
        }
    }
    for key, value := range kv.last {
        reply.Last[key] = value
    }
    return nil
}

func (kv *DisKV) Reconfiguration(new shardmaster.Config) bool {
    newdata := TranDataReply{Err: OK, Data: map[string] string{}, Last: map[string] string{}}
    old := &kv.config
    for shard, oldg := range old.Shards {
        newg := new.Shards[shard]
        if newg != oldg && newg == kv.gid {
            flag := false
            for _, server := range old.Groups[oldg] {
                args := TranDataArgs{Shard: shard, Num: old.Num}
                reply := TranDataReply{Err: OK}
                ok := call(server, "DisKV.TranData", &args, &reply)
                if ok && reply.Err == OK {
					for key, value := range reply.Data {
                        newdata.Data[key] = value
					}
					for key, value := range reply.Last {
						t, ok := newdata.Last[key]
						if !(ok && t >= value) {
                            newdata.Last[key] = value
						}
                    }
                    flag = true
                    break
                }
            }
            if flag == false && oldg > 0 {
                //fmt.Println("Reconfig---false", kv.gid, kv.me, new.Num, kv.config.Num)
                return false
            }
        }
    }
    //fmt.Println("Reconfig", kv.gid, kv.me, new.Num, kv.config.Num)
    op := Op{Type: "Reconfiguration", Config: new, NewData: newdata, Mark: rand.Int63()}
    kv.AddLog(op)
    return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	// Your code here.
	kv.mu.Lock()
    defer kv.mu.Unlock()
    oldnum := kv.config.Num
    newnum := kv.sm.Query(-1).Num
    for num := oldnum + 1; num <= newnum; num++ {
        config := kv.sm.Query(num)
        if kv.Reconfiguration(config) == false {
            return
        }
    }
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a diskv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	// Don't call Join().
	kv.now = 0
    kv.data = map[string] string {}
    kv.last = map[string] string {}
    kv.config = shardmaster.Config{Num: -1}

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)



	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
