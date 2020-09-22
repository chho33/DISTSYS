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
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}


func (mr *MapReduce) SpawnJob(job JobType) {

  var n int
  switch job {
  case Map:
    n = mr.nMap
  case Reduce:
    n = mr.nReduce
  }

  for i := 0; i<n; i++ {
    go mr.DoJob(job, i)
  }

  for i := 0; i<n; i++ {
    fmt.Println(i)
    <- mr.Done
  } 

}


func (mr *MapReduce) DoJob(job JobType, jobNumber int) {

  var other int
  switch job {
  case Map:
    other = mr.nReduce
  case Reduce:
    other = mr.nMap 
  }

  for {
    worker := <- mr.registerChannel

    var reply DoJobReply;
    args := &DoJobArgs{File: mr.file,
                       Operation: job,
                       JobNumber: jobNumber,
                       NumOtherPhase: other}

    ok := call(worker, "Worker.DoJob", args, &reply)

    if ok {
      mr.Done <- true 
      Register(mr.MasterAddress, worker)
      return
    } else {
      fmt.Printf("[%s] %s fail!\n", job, worker)
    }
  }

}


func (mr *MapReduce) RunMaster() *list.List {
  mr.SpawnJob(Map)
  mr.SpawnJob(Reduce)
  return mr.KillWorkers()
}
