package process

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPath(t *testing.T) {
	const (
		index    = 1
		faulty   = 1
		rounds   = 5
		numprocs = 5
	)

	dag := createDag(t, rounds, numprocs)
	p := NewForT(t, index, faulty)
	p.dag = dag

	t.Run("strong path consecutive rounds", func(t *testing.T) {
		from := vertexID{
			round:  3,
			source: 1,
		}
		to := vertexID{
			round:  2,
			source: 3,
		}

		require.True(t, p.path(from, to, true))
	})

	t.Run("strong path separated by 2 rounds", func(t *testing.T) {
		from := vertexID{
			round:  3,
			source: 3,
		}
		to := vertexID{
			round:  1,
			source: 4,
		}

		require.True(t, p.path(from, to, true))
	})

	t.Run("weak path", func(t *testing.T) {
		from := vertexID{
			round:  4,
			source: 1,
		}
		to := vertexID{
			round:  2,
			source: 4,
		}

		require.True(t, p.path(from, to, false))
	})

	t.Run("hybrid path", func(t *testing.T) {
		from := vertexID{
			round:  4,
			source: 1,
		}
		to := vertexID{
			round:  1,
			source: 1,
		}

		require.True(t, p.path(from, to, false))
	})

	t.Run("no path exists", func(t *testing.T) {
		from := vertexID{
			round:  3,
			source: 3,
		}
		to := vertexID{
			round:  2,
			source: 4,
		}

		require.False(t, p.path(from, to, false))
	})
}

// createDag creates a directed acyclic graph (DAG) as shown on Page 4 (Figure 1), DAG-Rider Paper.
func createDag(t *testing.T, rounds, numprocs int) [][]vertex {
	// Create test dag
	dag := make([][]vertex, rounds)
	for r := 0; r <= 4; r++ {
		dag[r] = make([]vertex, numprocs)
		for p := 1; p <= 4; p++ {
			dag[r][p] = vertex{
				id: vertexID{
					round:  r,
					source: p,
				},
			}
		}
	}

	// Set dags for round 1.
	dag[1][1].strongEdges = []vertexID{
		{
			round:  0,
			source: 1,
		},
		{
			round:  0,
			source: 2,
		},
		{
			round:  0,
			source: 3,
		},
	}
	dag[1][2].strongEdges = []vertexID{
		{
			round:  0,
			source: 1,
		},
		{
			round:  0,
			source: 2,
		},
		{
			round:  0,
			source: 3,
		},
	}
	dag[1][3].strongEdges = []vertexID{
		{
			round:  0,
			source: 1,
		},
		{
			round:  0,
			source: 2,
		},
		{
			round:  0,
			source: 3,
		},
	}
	dag[1][4].strongEdges = []vertexID{
		{
			round:  0,
			source: 1,
		},
		{
			round:  0,
			source: 2,
		},
		{
			round:  0,
			source: 3,
		},
	}

	// Set dags for round 2.
	dag[2][1].strongEdges = []vertexID{
		{
			round:  1,
			source: 1,
		},
		{
			round:  1,
			source: 2,
		},
		{
			round:  1,
			source: 4,
		},
	}
	dag[2][2].strongEdges = []vertexID{
		{
			round:  1,
			source: 1,
		},
		{
			round:  1,
			source: 2,
		},
		{
			round:  1,
			source: 4,
		},
	}
	dag[2][3].strongEdges = []vertexID{
		{
			round:  1,
			source: 1,
		},
		{
			round:  1,
			source: 3,
		},
		{
			round:  1,
			source: 4,
		},
	}
	dag[2][4].strongEdges = []vertexID{
		{
			round:  1,
			source: 1,
		},
		{
			round:  1,
			source: 2,
		},
		{
			round:  1,
			source: 4,
		},
	}

	// Set dags for round 3.
	dag[3][1].strongEdges = []vertexID{
		{
			round:  2,
			source: 1,
		},
		{
			round:  2,
			source: 3,
		},
	}
	dag[3][2].strongEdges = []vertexID{
		{
			round:  2,
			source: 1,
		},
		{
			round:  2,
			source: 2,
		},
		{
			round:  2,
			source: 3,
		},
	}
	dag[3][3].strongEdges = []vertexID{
		{
			round:  2,
			source: 1,
		},
		{
			round:  2,
			source: 2,
		},
		{
			round:  2,
			source: 3,
		},
	}

	// Set dags for round 4.
	dag[4][1].strongEdges = []vertexID{
		{
			round:  3,
			source: 1,
		},
		{
			round:  3,
			source: 2,
		},
		{
			round:  3,
			source: 3,
		},
	}

	// Set weak edges.
	dag[4][1].weakEdges = []vertexID{
		{
			round:  2,
			source: 4,
		},
	}

	return dag
}
