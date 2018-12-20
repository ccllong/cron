package cron

import (
	"log"
	"runtime"
	"sort"
	"time"
)

//标识当前cron模块已占用的协程数
var currentGoroutines = 0

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries      []*Entry
	stop         chan struct{}
	add          chan *Entry
	snapshot     chan []*Entry
	running      bool
	ErrorLog     *log.Logger
	location     *time.Location
	MaxGoroutine int //用于限制整个定时任务的goroutine协程总数，为0时表示不限制，当不为0时，会进行限制且有一种风险，就是如果某个定时任务中间阻塞了，将会影响其它任务的执行时间精准度，即启用限制总数情况下，AB两任务，A任务如果阻塞了N秒，则在协和数不够用的状态下，B任务的执行精度也会受影响阻塞N秒
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, in the Local time zone.
func New(maxGoroutine int) *Cron {
	return NewWithLocation(time.Now().Location(), maxGoroutine)
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location, maxGoroutine int) *Cron {
	return &Cron{
		entries:      nil,
		add:          make(chan *Entry),
		stop:         make(chan struct{}),
		snapshot:     make(chan []*Entry),
		running:      false,
		ErrorLog:     nil,
		location:     location,
		MaxGoroutine: maxGoroutine,
	}
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// 将 job 加入 Cron 中
// 如上所述，该方法只是简单的通过 FuncJob 类型强制转换 cmd，然后调用 AddJob 方法
// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func()) error {
	return c.AddJob(spec, FuncJob(cmd))
}

// 将 job 加入 Cron 中
// 通过 Parse 函数解析 cron 表达式 spec 的到调度器实例(Schedule)，之后调用 c.Schedule 方法
// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) error {
	schedule, err := Parse(spec)
	if err != nil {
		return err
	}
	c.Schedule(schedule, cmd)
	return nil
}

// 通过两个参数实例化一个 Entity，然后加入当前 Cron 中
// 注意：如果当前 Cron 未运行，则直接将该 entity 加入 Cron 中；
// 否则，通过 add 这个成员 channel 将 entity 加入正在运行的 Cron 中
// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// 获取当前 Cron 总所有 Entities 的快照
// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// 新启动一个 goroutine 运行当前 Cron
// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	if c.running {
		return
	}
	c.running = true
	currentGoroutines++
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	if c.running {
		return
	}
	c.running = true
	c.run()
}

func (c *Cron) runWithRecovery(j Job, isNew bool) {
	defer func() {
		if isNew {
			currentGoroutines--
		}
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()
	j.Run()
}

//实时系统协程总数 runtime.NumGoroutine()
// Run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					//当前跑的协程总数小于设置的最大值时，新开新协程跑,否则正常跑
					if c.MaxGoroutine == 0 || (currentGoroutines < c.MaxGoroutine) {
						currentGoroutines++
						go c.runWithRecovery(e.Job, true)
					} else {
						c.runWithRecovery(e.Job, false)
					}
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// 通过给 stop 成员发送一个 struct{}{} 来停止当前 Cron，同时将 running 置为 false
// 从这里知道，stop 只是通知 Cron 停止，因此往 channel 发一个值即可，而不关心值是多少
// 所以，成员 stop 定义为空 struct
// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

//获取当前Cron占用的协程数
func (c *Cron) CurrentNumGoroutines() int {
	return currentGoroutines
}
