package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) SendJob(worker string, id int, operation JobType) bool {
    var args DoJobArgs
    var reply DoJobReply
    args.File = mr.file
    args.Operation = operation
    args.JobNumber = id
    switch operation {
    case Map: 
        args.NumOtherPhase = mr.nReduce
    case Reduce:
        args.NumOtherPhase = mr.nMap
    }
    return call(worker, "Worker.DoJob", args, &reply)
}

func (mr *MapReduce) ArrangeJob(id int, operation JobType) {
    for {
         var worker string
	     var ok bool = false
	     select {
	     case worker = <-mr.idleChannel:
	        ok = mr.SendJob(worker, id, operation)
	     case worker = <-mr.registerChannel:
	        ok = mr.SendJob(worker, id, operation)
	     }
	     if (ok) {
	        switch operation {
	        case Map:
	            mr.mapChannel <- id
	        case Reduce:
	            mr.reduceChannel <- id
	        }
	        mr.idleChannel <- worker
	        return
	     }
    }
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	for i := 0; i<mr.nMap; i++ {
	    go mr.ArrangeJob(i, Map)
	}
	for i := 0; i<mr.nMap; i++ {
	    <-mr.mapChannel
	}
	for i := 0; i<mr.nReduce; i++ {
	    go mr.ArrangeJob(i, Reduce)
	}
	for i := 0; i<mr.nReduce; i++ {
	    <-mr.reduceChannel
	}
	return mr.KillWorkers()
}
