package gorp

type Queue struct {
	fifo      []string
	maxLength int
}

func NewQueue(maxLength int) *Queue {
	return &Queue{
		maxLength: maxLength,
	}
}

func (q *Queue) Push(s string) {
	q.fifo = append(q.fifo, s)
	if q.maxLength > 0 && q.Len() > q.maxLength {
		q.Pop() // discard overflow
	}
}

func (q *Queue) Pop() (s string) {
	// demo
	if len(q.fifo) > 0 {
		s = (q.fifo)[0]
		q.fifo = (q.fifo)[1:]
	}
	return
}

func (q *Queue) Len() int {
	return len(q.fifo)
}
