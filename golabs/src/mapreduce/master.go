package mapreduce

import "container/list"
import (
	"fmt"
	"strconv"
)


type WorkerInfo struct {
	address 	string
	// You can add definitions here.
	idle		bool
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

//func sendRestartArgs(restartArgs *DoJobArgs, mr *MapReduce) {
//	mr.restartChannel <- restartArgs
//}
func initJobQueue(mr *MapReduce) (*list.List, *list.List) {
	mapJobList := list.New()
	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.JobNumber = i
		args.Operation = Map
		args.NumOtherPhase = mr.nReduce
		mapJobList.PushBack(args)
	}

	ReduceJobList := list.New()
	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.JobNumber = i
		args.Operation = Reduce
		args.NumOtherPhase = mr.nMap
		ReduceJobList.PushBack(args)
	}

	return mapJobList, ReduceJobList
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	count := 0
	mr.Workers = make(map[string]*WorkerInfo)

	mapJobList, reduceJobList := initJobQueue(mr)
	for mapJobList.Len() != 0 || reduceJobList.Len() != 0 {
		select {
			// Register a worker into registerChannel
			case workerAddress := <- mr.registerChannel:
				workerName := "worker" + strconv.Itoa(count)
				mr.Workers[workerName] = new(WorkerInfo)
				mr.Workers[workerName].address = workerAddress
				mr.Workers[workerName].idle = true
				count++
			default:
				for _, w := range mr.Workers {
					if (w.idle) {
						w.idle = false
						var args *DoJobArgs
						var reply ShutdownReply
						if mapJobList.Len() != 0 {

							args = mapJobList.Front().Value.(*DoJobArgs)
							if args != nil {
								mapJobList.Remove(mapJobList.Front())
							}
						} else if reduceJobList.Len() != 0 {
							args = reduceJobList.Front().Value.(*DoJobArgs)
							if args != nil{
								reduceJobList.Remove(reduceJobList.Front())
							}
						}

						if args != nil {
							ok := call(w.address, "Worker.DoJob", args, &reply)
							w.idle = true
							if ok == false  {
								if args.Operation == Map {
									mapJobList.PushBack(args)
									fmt.Println(mapJobList.Len())
								} else if args.Operation == Reduce {
									reduceJobList.PushBack(args)
									fmt.Println(reduceJobList.Len())
								}
							}
						}

					}
				}
		}
	}

	return mr.KillWorkers()
}

