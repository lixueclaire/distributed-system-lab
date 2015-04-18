package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View
	content    map[string] string
    forward    bool
}

func (pb *PBServer) isPrimary() bool {
    return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
    return pb.view.Backup == pb.me
}

func (pb *PBServer) hasPrimary() bool {
    return pb.view.Primary != ""
}

func (pb *PBServer) hasBackup() bool {
    return pb.view.Backup != ""
}
    
//forward to backup

func (pb *PBServer) ForwardAll(args *ForwardArgs) error{
    if !pb.hasBackup() {
        return nil
    }
    var reply ForwardReply
    ok := call(pb.view.Backup, "PBServer.ReplyForwardCall", args, &reply)
    if !ok {
       return errors.New("Forward failed")
    }
    return nil
}
    
func (pb *PBServer) Forward(args *ForwardArgs) error{
    if !pb.hasBackup() || pb.forward == true {
        return nil
    }
    var reply ForwardReply
    ok := call(pb.view.Backup, "PBServer.ReplyForwardCall", args, &reply)
    if !ok {
        return errors.New("Forward failed")
    }
    return nil
}

//reply to a forward request

func (pb *PBServer) ReplyForwardCall(args *ForwardArgs, reply *ForwardReply) error {
    pb.mu.Lock()
    if !pb.isBackup() {
        pb.mu.Unlock()
        return errors.New("it's not backup")
    }
    for key, value := range args.Content {
        if args.Op == "Append" {
            if args.Client != "" && pb.content[args.Client] != args.Id {
                pb.content[key] += value
            }
        } else {
            pb.content[key] = value
        }
    }
    if args.Client != "" {
        pb.content[args.Client] = args.Id
    }
    pb.mu.Unlock()
    return nil
}
    
    

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
    pb.mu.Lock()
    if !pb.isPrimary() {
        reply.Err = ErrWrongServer
        pb.mu.Unlock()
        return errors.New("it's not primary, but received Get")
    }
    reply.Value = pb.content[args.Key]
    pb.mu.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
    pb.mu.Lock()
    if !pb.isPrimary() {
        reply.Err = ErrWrongServer
        pb.mu.Unlock()
        return errors.New("it's not primary, but received Put")
    }
    key, value, client, id, op:= args.Key, args.Value, args.Me, args.Id, args.Op
    if pb.content["last operation of " + client] == id {
        pb.mu.Unlock()
        return nil
    }
    forwardargs := &ForwardArgs{map[string] string{key: value}, "last operation of " + client, id, op}
    err := pb.Forward(forwardargs)
    if err != nil {
        pb.mu.Unlock()
        return errors.New("Forward failed")
    }
    if (args.Op == "Append") {
        pb.content[key] += value
    } else {
        pb.content[key] = value
    }
    pb.content["last operation of " + client] = id
    pb.mu.Unlock()
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	view, error := pb.vs.Ping(pb.view.Viewnum)
	if error != nil {
       // do nothing
	}
	if !pb.isPrimary() {
        pb.forward = false
    }
	if pb.isPrimary() && view.Backup != "" && pb.view.Backup != view.Backup {
	    pb.forward = true
	}
    pb.view = view
	if pb.forward {
        err := pb.ForwardAll(&ForwardArgs{Content:pb.content})
        if err != nil {
            fmt.Println("init backup failed")
        } else {
            pb.forward = false
        }
	}
    pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.content = map[string] string {}
    pb.view = viewservice.View{}
    pb.forward = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
