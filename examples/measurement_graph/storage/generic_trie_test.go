package storage_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stephen-mcelhose/go-streaming/examples/measurement_graph/storage"
)

func TestGenericPrefixTrie_Spec(t *testing.T) {
	t.Run("MUST put and get generic types at arbitrary string paths", func(t *testing.T) {
		trie := storage.NewGenericPrefixTrie[string, int]()
		defer trie.Close()

		path1 := []string{"users", "us-east", "1001"}
		path2 := []string{"users", "us-west", "2002"}

		trie.Put(path1, 42)
		trie.Put(path2, 99)

		v1, ok1 := trie.Get(path1)
		require.True(t, ok1)
		require.Equal(t, 42, v1)

		v2, ok2 := trie.Get(path2)
		require.True(t, ok2)
		require.Equal(t, 99, v2)

		_, ok3 := trie.Get([]string{"users", "non-existent"})
		require.False(t, ok3)
		require.Equal(t, 2, trie.Count())
	})

	t.Run("MUST collect values under prefix", func(t *testing.T) {
		trie := storage.NewGenericPrefixTrie[string, string]()
		defer trie.Close()

		trie.Put([]string{"metrics", "cpu", "host1"}, "val1")
		trie.Put([]string{"metrics", "cpu", "host2"}, "val2")
		trie.Put([]string{"metrics", "mem", "host1"}, "val3")

		cpuVals := trie.CollectPrefix("metrics", "cpu")
		require.Len(t, cpuVals, 2)
		require.Contains(t, cpuVals, "val1")
		require.Contains(t, cpuVals, "val2")

		allVals := trie.CollectAll()
		require.Len(t, allVals, 3)
	})

	t.Run("MUST invalidate prefix subtree in O(1)", func(t *testing.T) {
		trie := storage.NewGenericPrefixTrie[string, int]()
		defer trie.Close()

		for i := 1; i <= 50; i++ {
			trie.Put([]string{"fleet", "sensor_a", fmt.Sprintf("dev_%d", i)}, i)
		}
		trie.Put([]string{"fleet", "sensor_b", "dev_1"}, 100)

		require.Equal(t, 51, trie.Count())

		pruned := trie.InvalidatePrefix("fleet", "sensor_a")
		require.Equal(t, 50, pruned)
		require.Equal(t, 1, trie.Count())

		v, ok := trie.Get([]string{"fleet", "sensor_b", "dev_1"})
		require.True(t, ok)
		require.Equal(t, 100, v)
	})

	t.Run("MUST detach prefix subtree into standalone generic trie in O(1)", func(t *testing.T) {
		trie := storage.NewGenericPrefixTrie[string, int]()
		defer trie.Close()

		for i := 1; i <= 20; i++ {
			trie.Put([]string{"region", "east", fmt.Sprintf("node_%d", i)}, i)
		}
		trie.Put([]string{"region", "west", "node_1"}, 99)

		detached := trie.DetachPrefix("region", "east")
		defer detached.Close()

		require.Equal(t, 20, detached.Count())
		require.Equal(t, 1, trie.Count())

		for i := 1; i <= 20; i++ {
			v, ok := detached.Get([]string{"region", "east", fmt.Sprintf("node_%d", i)})
			require.True(t, ok)
			require.Equal(t, i, v)
		}
	})

	t.Run("MUST execute concurrent writes and reads without data races", func(t *testing.T) {
		trie := storage.NewGenericPrefixTrieWithShards[string, int](64)
		defer trie.Close()

		const goroutines = 32
		const perRoutine = 100

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for g := 0; g < goroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < perRoutine; i++ {
					trie.Put([]string{"shard", fmt.Sprintf("tenant_%d", gid), fmt.Sprintf("key_%d", i)}, i)
				}
			}(g)
		}
		wg.Wait()

		require.Equal(t, goroutines*perRoutine, trie.Count())
	})
}
