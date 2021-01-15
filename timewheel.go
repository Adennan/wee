package wee

import (
	"container/list"
	"time"
)

var (
	add    chan Context
	remove chan interface{}
)

// Task callback function for delay
type Task func(interface{})

// TimeWheel time wheel
type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List
	timer    map[interface{}]int
	position int
	task     Task
	state    bool
}

// Context context
type Context struct {
	timeout time.Duration
	circle  int
	key     interface{}
	data    interface{}
}

// New create a time wheel
func New(interval time.Duration, slotNum int, task Task) *TimeWheel {
	if interval <= 0 || slotNum <= 0 || task == nil {
		return nil
	}
	tw := &TimeWheel{
		interval: interval,
		slots:    make([]*list.List, slotNum),
		timer:    make(map[interface{}]int),
		position: 0,
		task:     task,
		state:    true,
	}
	tw.initSlots(slotNum)
	return tw
}

func (tw *TimeWheel) initSlots(num int) {
	for i := 0; i < num; i++ {
		tw.slots[i] = list.New()
	}
}

// Start run timer
func (tw *TimeWheel) Start() {
	tw.state = true
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stop timer
func (tw *TimeWheel) Stop() {
	tw.state = false
	tw.ticker.Stop()
}

// AddTimer add timer in time wheel
func (tw *TimeWheel) AddTimer(timeout time.Duration, key interface{}, job interface{}) {
	if key != nil {
		add <- Context{timeout: timeout, key: key, data: job}
	}
}

// RemoveTimer remove timer in time wheel
func (tw *TimeWheel) RemoveTimer(key interface{}) {
	if key != nil {
		remove <- key
	}
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.handler()
		case job := <-add:
			tw.addTimer(job)
		case key := <-remove:
			tw.removeTimer(key)
		}
	}
}

func (tw *TimeWheel) handler() {
	l := tw.slots[tw.position]
	tw.checkExpired(l)
	if tw.position == len(tw.slots)-1 {
		tw.position = 0
	} else {
		tw.position++
	}
}

func (tw *TimeWheel) checkExpired(l *list.List) {
	for node := l.Front(); node != nil; {
		ctx := node.Value.(*Context)
		if ctx.circle > 0 {
			ctx.circle--
			node = node.Next()
			continue
		}

		go tw.task(ctx.data)
		next := node.Next()
		l.Remove(node)
		if ctx.key != nil {
			delete(tw.timer, ctx.key)
		}
		node = next
	}
}

func (tw *TimeWheel) addTimer(ctx Context) {
	position, circle := tw.GetPosition(ctx.timeout)
	ctx.circle = circle
	tw.slots[position].PushBack(ctx)

	if ctx.key != nil {
		tw.timer[ctx.key] = position
	}
}

func (tw *TimeWheel) removeTimer(key interface{}) {
	position := tw.timer[key]
	l := tw.slots[position]
	for node := l.Front(); node != nil; {
		ctx := node.Value.(*Context)
		if ctx.key == key {
			delete(tw.timer, ctx.key)
			l.Remove(node)
		}
		node = node.Next()
	}
}

// GetPosition get the position and circles of timer
func (tw *TimeWheel) GetPosition(timeout time.Duration) (int, int) {
	delay := int(timeout.Seconds())
	interval := int(tw.interval.Seconds())
	circle := int(delay / (interval * len(tw.slots)))
	position := int(tw.position+delay/interval) % len(tw.slots)
	return position, circle
}
