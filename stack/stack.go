package stack

func New[T any]() Stack[T] {
	d := make([]T, 0, 16)

	return Stack[T]{
		data: d,
	}
}

type Stack[T any] struct {
	data []T
}

func (s *Stack[T]) IsEmpty() bool {
	return len(s.data) == 0
}

func (s *Stack[T]) Push(e T) {
	s.data = append(s.data, e)
}

func (s *Stack[T]) Pop() T {
	dataLen := len(s.data)
	e := s.data[dataLen-1]
	s.data = s.data[0 : dataLen-1]

	return e
}
