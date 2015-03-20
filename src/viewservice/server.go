package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	view     View //current view
	primaryAck  uint
	backupAck   uint
	primaryTick uint
	backupTick  uint
	currentTick uint
}

func (vs *ViewServer) initPrimary() {
    vs.view.Primary = ""
    vs.primaryAck = 0
    vs.primaryTick = 0
}

func (vs *ViewServer) initBackup() {
    vs.view.Backup = ""
    vs.backupAck = 0
    vs.backupTick = 0
}

func (vs *ViewServer) ChangePrimary() {
    if (vs.view.Backup == "") {
        vs.initPrimary()
        return;
    }
    vs.view.Primary = vs.view.Backup
    vs.primaryAck = vs.backupAck
    vs.primaryTick = vs.backupTick
    vs.initBackup()
    vs.view.Viewnum ++
}

func (vs *ViewServer) isAcked() bool {
    return vs.view.Viewnum == vs.primaryAck
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	name := args.Me
    num := args.Viewnum
    vs.mu.Lock()
    if vs.view.Primary == "" && vs.view.Viewnum == 0 { //new primary
        vs.view.Primary = name
        vs.primaryAck = num
        vs.primaryTick = vs.currentTick
        vs.view.Viewnum ++
    } else if name == vs.view.Primary { //ping from primary
        if num == 0 { //primary restart
            vs.ChangePrimary() 
        } else {
            vs.primaryAck = num
            vs.primaryTick = vs.currentTick
        }
    } else if vs.view.Backup == "" && vs.isAcked() { //new backup 
        vs.view.Backup = name
        vs.backupAck = num
        vs.backupTick = vs.currentTick
        vs.view.Viewnum++
    } else if name == vs.view.Backup { //ping from backup
        if num == 0 { //backup restart
            if vs.isAcked() {
                vs.view.Viewnum++
                vs.backupAck = num
                vs.backupTick = vs.currentTick
            }
        } else {
            vs.backupAck = num
            vs.backupTick = vs.currentTick
        }    
    }
    reply.View = vs.view
    vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
    vs.mu.Lock()
    reply.View = vs.view
    vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
    
	// Your code here.
	vs.mu.Lock()
	vs.currentTick++
	if vs.view.Primary != "" && vs.currentTick - vs.primaryTick  >= DeadPings && vs.isAcked() {
	    vs.ChangePrimary()
	}
	if vs.view.Backup != "" && vs.currentTick - vs.backupTick >= DeadPings && vs.isAcked() {
	    vs.initBackup()
	    vs.view.Viewnum++
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
    vs.view.Viewnum = 0
    vs.initPrimary()
    vs.initBackup()
    
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
