package stack_test

import (
	"github.com/stretchr/testify/require"
	"github.com/xenowits/dag-rider/stack"
	"testing"
)

func TestStack(t *testing.T) {
	s := stack.New[int]()
	require.True(t, s.IsEmpty())

	s.Push(1)
	s.Push(2)
	require.Equal(t, s.Pop(), 2)
	require.Equal(t, s.Pop(), 1)
	require.True(t, s.IsEmpty())
}
