package main

type workerPool struct {
	maxWorker   int
	queuedTaskC chan func()
}

func NewWorkerPool(poolSize int) *workerPool {
	wp := workerPool{maxWorker: poolSize}
	wp.queuedTaskC = make(chan func(), 1000)
	return &wp
}

func (wp *workerPool) Run() {
	for i := 0; i < wp.maxWorker; i++ {
		go func() {
			for task := range wp.queuedTaskC {
				task()
			}
		}()
	}
}

func (wp *workerPool) AddTask(task func()) {
	wp.queuedTaskC <- task
}
