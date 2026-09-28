package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrStateNotFound 表示该批次此前从未持久化过（首次提交）。
var ErrStateNotFound = errors.New("batch state not found")

// Store 是批次状态的持久化接口。
// 实现必须保证 Load 与 Save 的串行化语义，使得：
//   - 进程重启后能读回上次的终态；
//   - 相同 BatchID 的重复提交看到同一份状态；
//   - Save 以原子方式整体落盘（调用方据此保证不会出现半个状态文件）。
type Store interface {
	Load(ctx context.Context, batchID BatchID) (*BatchState, error)
	Save(ctx context.Context, state *BatchState) error
}

// FileStore 基于 JSON 文件的 Store 实现（状态文件按批次隔离）。
type FileStore struct {
	// Dir 为状态文件所在目录。
	Dir string

	mu sync.Mutex
}

// NewFileStore 创建一个目录位于 dir 的文件状态存储。
func NewFileStore(dir string) *FileStore { return &FileStore{Dir: dir} }

func (s *FileStore) path(batchID BatchID) string {
	// 对批次 ID 做最基本的文件名安全约束。
	name := fmt.Sprintf("batch-%s.json", string(batchID))
	return filepath.Join(s.Dir, filepath.Base(name))
}

func (s *FileStore) Load(ctx context.Context, batchID BatchID) (*BatchState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.path(batchID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("读取批次状态 %s: %w", batchID, err)
	}
	var st BatchState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("解析批次状态 %s: %w", batchID, err)
	}
	if st.Jobs == nil {
		st.Jobs = make(map[JobID]*JobState)
	}
	return &st, nil
}

func (s *FileStore) Save(ctx context.Context, state *BatchState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return fmt.Errorf("创建状态目录: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化批次状态: %w", err)
	}

	path := s.path(state.BatchID)
	tmp, err := os.CreateTemp(s.Dir, ".batch-*.tmp")
	if err != nil {
		return fmt.Errorf("创建临时状态文件: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 一个不存在的文件是无副作用的 no-op

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("写入临时状态文件: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("刷盘临时状态文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("关闭临时状态文件: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("原子替换状态文件: %w", err)
	}
	return nil
}
