package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const ScheduleInterval = time.Second

type FileStatus int32

const (
	UNPROCESS  FileStatus = 0
	PROCESSING FileStatus = 1
	FINISHED   FileStatus = 2
)

type TaskType int32

const (
	Map    TaskType = 0
	Reduce TaskType = 1
)

type TaskStatus int32

const (
	UNSTARTED TaskStatus = 0
	RUNNING   TaskStatus = 1
	FAILED    TaskStatus = 2
	END       TaskStatus = 4
)

type TaskStat struct {
	WorkerId  int
	Status    TaskStatus
	StartTime time.Time
}

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Queue struct {
	items []int
	lock  sync.RWMutex
	cond  *sync.Cond
}

func NewQueue() *Queue {
	q := Queue{}
	q.items = make([]int, 0)
	q.cond = sync.NewCond(&sync.Mutex{})
	return &q
}

func (q *Queue) New() {
	q.items = make([]int, 0)
}

func (q *Queue) Push(item int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.items = append(q.items, item)
	q.cond.Broadcast()
}

func (q *Queue) Pop() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if len(q.items) == 0 {
		return -1
	}
	ret := q.items[0]
	q.items = q.items[1:]
	return ret
}

func (q *Queue) Empty() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.items) == 0
}

type Coordinator struct {
	// Your definitions here.
	mu       sync.Mutex
	doneLock sync.Mutex
	cond     *sync.Cond
	nReduce  int

	// 记录剩余未被处理的文件
	fileMap  map[string]FileStatus
	fileTask map[int]string

	// 记录worker，用于register时分配id
	WorkerCounter int

	// 记录MapTask的状态
	MapTask map[int]*TaskStat
	// 记录ReduceTask的状态
	ReduceTask map[int]*TaskStat
	// 处于等待状态的worker
	WaitingQueue *Queue
	// Map or Reduce
	taskPhase TaskPhase
	// 用于同步
	ChanMap map[int]chan bool

	rCounter int
	mCounter int

	done bool
}

// coordinator should notice if a worker hasn't completed its task in 10 seconds.
// and then give this task to a different worker.

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// when a worker send an RPC to ask for a task, coordinator should respond with the filename of an as-yet-unstarted
// map task.

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond.
//Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
//coordinator wait for ten seconds; after that the coordinator should
//assume the worker has died (of course, it might not have).
func (c *Coordinator) Get(args int, reply *GetTaskReply) error {
	reply.NReduce = c.nReduce
	c.checkTask(args, reply)
	return nil
}

func (c *Coordinator) checkTask(workerid int, reply *GetTaskReply) {
	c.mu.Lock()
	fmt.Printf("CheckTask: workerid: %d\n\n", workerid)
	if c.taskPhase == MapPhase {
		c.mCounter++
		for filename, status := range c.fileMap {
			if status == UNPROCESS {
				c.fileTask[c.mCounter] = filename
				c.fileMap[filename] = PROCESSING
				c.MapTask[c.mCounter] = &TaskStat{workerid, RUNNING, time.Now()}
				reply.Task = Map
				reply.Filename = filename
				reply.TaskId = c.mCounter
				c.mu.Unlock()
				return
			}
		}
		c.mu.Unlock()
		// 将这个worker加入等待队列
		fmt.Printf("Waiting: %d\n", workerid)
		c.WaitingQueue.Push(workerid)
		c.cond.Wait()
		fmt.Printf("worker %v has been awoke\n", workerid)
		c.checkTask(workerid, reply)
		//if ok := <-c.ChanMap[workerid]; ok {
		//
		//}
	} else {
		if c.rCounter >= c.nReduce {
			for rCounter, status := range c.ReduceTask {
				if status.Status == FAILED || status.Status == UNSTARTED {
					reply.TaskId = rCounter
					reply.MapCount = len(c.fileMap)
					reply.Task = Reduce
					c.ReduceTask[rCounter] = &TaskStat{workerid, RUNNING, time.Now()}
					c.mu.Unlock()
					return
				}
			}
			// 我们可以这样设计，worker id用于标识每一个worker，但是使用rCounter和wCounter来标识不同的task
			c.WaitingQueue.Push(workerid)
			c.mu.Unlock()
			c.cond.Wait()
			fmt.Printf("worker %v has been awoke\n", workerid)
			c.checkTask(workerid, reply)
			//if ok := <-c.ChanMap[workerid]; ok {
			//
			//
			//}
		} else {
			c.rCounter++
			reply.TaskId = c.rCounter
			reply.MapCount = len(c.fileMap)
			reply.Task = Reduce
			c.ReduceTask[c.rCounter] = &TaskStat{workerid, RUNNING, time.Now()}
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) Register(args UNUSED, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.WorkerCounter++
	*reply = c.WorkerCounter
	return nil
}

func (c *Coordinator) ReduceReport(args ReduceReport, reply *UNUSED) error {
	// 如何判断某一个reduce task有没有执行完
	c.mu.Lock()
	c.ReduceTask[args.Rno].Status = END
	var str string
	switch c.ReduceTask[args.Rno].Status {
	case END:
		str = "END"
		break
	case UNSTARTED:
		str = "UNSTARTED"
		break
	case FAILED:
		str = "FAILED"
		break
	case RUNNING:
		str = "RUNNING"
	}
	fmt.Printf("ReduceReport: TaskId: %v, WorkerId: %v, status: %v\n", args.Rno, c.ReduceTask[args.Rno].WorkerId, str)
	flag := true
	for k, v := range c.ReduceTask {
		if v.Status != END {
			switch v.Status {
			case END:
				str = "END"
				break
			case UNSTARTED:
				str = "UNSTARTED"
				break
			case FAILED:
				str = "FAILED"
				break
			case RUNNING:
				str = "RUNNING"
			}
			fmt.Printf("TaskId: %v, WorkerId: %v, status: %v\n", k, v.WorkerId, str)
			flag = false
			break
		}
	}
	c.mu.Unlock()
	c.doneLock.Lock()
	c.done = flag
	fmt.Printf("c.Done: %v\n\n", c.done)
	c.doneLock.Unlock()
	return nil
}

func (c *Coordinator) MapReport(args MapReport, reply *UNUSED) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MapTask[args.Mno].Status = END
	filename := c.fileTask[args.Mno]
	c.fileMap[filename] = FINISHED
	if c.fileMap[filename] == FINISHED {
		fmt.Printf("MapReport: TaskId: %v, WorkerId: %v, filename: %v, status: FINISHED\n", args.Mno, c.MapTask[args.Mno].WorkerId, filename)
	}
	for k, v := range c.fileMap {
		if v != FINISHED {
			fmt.Printf("filename: %v, status: %v\n", k, v)
			return nil
		}
	}
	c.taskPhase = ReducePhase
	fmt.Println("task phase switched.")
	c.taskReschedule(-1)
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.doneLock.Lock()
	defer c.doneLock.Unlock()
	if c.done {
		fmt.Println("Done")
	}
	return c.done
}

// Lock是对于goroutine来说的，而不是对于一个函数来说的
// 考虑如下执行路径：schedule->taskReschedule->唤醒阻塞的协程->return
func (c *Coordinator) schedule() {
	if c.Done() {
		return
	}
	var taskmap map[int]*TaskStat
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskPhase == MapPhase {
		taskmap = c.MapTask
	} else {
		taskmap = c.ReduceTask
	}
	for taskid, t := range taskmap {
		if t.Status == RUNNING {
			if time.Now().After(t.StartTime.Add(time.Second * 10)) {
				t.Status = FAILED
				c.taskReschedule(taskid)
			}
		}
	}
}

// 需要唤醒线程的时机：出现FAILED，从MapPhase切换到ReducePhase

func (c *Coordinator) taskReschedule(taskid int) {
	fmt.Printf("taskReschedule entered\n")
	if c.taskPhase == MapPhase {
		filename := c.fileTask[taskid]
		c.fileMap[filename] = UNPROCESS
	}
	// Reduce failed如何处理？
	c.awakeRoutine()
	fmt.Printf("taskReschedule end")
}

func (c *Coordinator) awakeRoutine() {
	// 选择一个处于WAITING状态的worker唤醒，唤醒后由其自行选择一个需要的任务来完成。
	fmt.Printf("awakeRoutine entered\n")
	//waitingWorker := c.WaitingQueue.Pop()
	fmt.Println()
	c.cond.Broadcast()
	//if waitingWorker != -1 {
	//	c.ChanMap[waitingWorker] <- true
	//}
	fmt.Printf("awakeRoutine ended\n")
}

func (c *Coordinator) tickSchedule() {
	for !c.Done() {
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//if len(files) < 2 {
	//	fmt.Fprintf(os.Stderr, "usage: MapReduce xxx.so inputfiles...\n")
	//	os.Exit(1)
	//}
	c := Coordinator{}
	// 应该记录所有待处理的文件，文件的状态包括未处理，处理中，和已处理完成，还要保存正在处理的worker id。
	// 如何区分不同的worker？
	// Your code here.
	c.nReduce = nReduce
	c.fileMap = make(map[string]FileStatus)
	c.ReduceTask = make(map[int]*TaskStat)
	c.MapTask = make(map[int]*TaskStat)
	c.fileTask = make(map[int]string)
	c.ChanMap = make(map[int]chan bool)
	c.WaitingQueue = NewQueue()
	for _, filename := range files {
		c.fileMap[filename] = UNPROCESS
	}
	c.taskPhase = MapPhase
	c.ReduceTask[0] = &TaskStat{0, END, time.Now()}
	for i := 1; i <= nReduce; i++ {
		c.ReduceTask[i] = &TaskStat{0, UNSTARTED, time.Now()}
	}
	c.done = false
	c.cond = sync.NewCond(&sync.Mutex{})
	go c.tickSchedule()
	c.server()
	return &c
}

// 调度器轮转，检查map中每个任务的时间，如果超时，那么将这个worker设置为failed。如果结束，直接将这个删掉？
