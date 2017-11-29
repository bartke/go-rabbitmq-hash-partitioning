package gorp

import "testing"

func TestFIFO(t *testing.T) {
	q := NewQueue(0)

	if q.Len() != 0 {
		t.Errorf("expected queue to be of length %d but was %d", 0, q.Len())
	}

	q.Push("a")
	q.Push("b")
	q.Push("c")

	if q.Len() != 3 {
		t.Errorf("expected queue to be of length %d but was %d", 3, q.Len())
	}

	if item := q.Pop(); item != "a" {
		t.Errorf("expected queue to pop %s but was %s", "a", item)
	}

	if item := q.Pop(); item != "b" {
		t.Errorf("expected queue to pop %s but was %s", "b", item)
	}

	if item := q.Pop(); item != "c" {
		t.Errorf("expected queue to pop %s but was %s", "c", item)
	}

	if item := q.Pop(); item != "" {
		t.Errorf("expected queue to pop %s but was %s", "", item)
	}
}

func TestFIFOWithSize(t *testing.T) {
	q := NewQueue(2)
	q.Push("a")
	q.Push("b")
	q.Push("c")

	if q.Len() != 2 {
		t.Errorf("expected queue to be of length %d but was %d", 2, q.Len())
	}

	if item := q.Pop(); item != "b" {
		t.Errorf("expected queue to pop %s but was %s", "b", item)
	}

	if item := q.Pop(); item != "c" {
		t.Errorf("expected queue to pop %s but was %s", "c", item)
	}

	if item := q.Pop(); item != "" {
		t.Errorf("expected queue to pop %s but was %s", "", item)
	}
}
