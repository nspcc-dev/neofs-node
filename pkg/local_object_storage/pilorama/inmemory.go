package pilorama

// nodeInfo couples parent and metadata.
type nodeInfo struct {
	Parent Node
	Meta   Meta
}

type move struct {
	Move
	HasOld bool
	Old    nodeInfo
}

// state represents state being replicated.
type state struct {
	operations []move
	tree
}

// newState constructs new empty tree.
func newState() *state {
	return &state{
		tree: *newTree(),
	}
}

// undo un-does op and changes s in-place.
func (s *state) undo(op *move) {
	children := s.tree.childMap[op.Parent]
	for i := range children {
		if children[i] == op.Child {
			if len(children) > 1 {
				s.tree.childMap[op.Parent] = append(children[:i], children[i+1:]...)
			} else {
				delete(s.tree.childMap, op.Parent)
			}
			break
		}
	}

	if op.HasOld {
		s.tree.infoMap[op.Child] = op.Old
		oldChildren := s.tree.childMap[op.Old.Parent]
		for i := range oldChildren {
			if oldChildren[i] == op.Child {
				return
			}
		}
		s.tree.childMap[op.Old.Parent] = append(oldChildren, op.Child)
	} else {
		delete(s.tree.infoMap, op.Child)
	}
}

// Apply puts op in log at a proper position, re-applies all subsequent operations
// from log and changes s in-place.
func (s *state) Apply(op *Move) error {
	var index int
	for index = len(s.operations); index > 0; index-- {
		if s.operations[index-1].Time <= op.Time {
			break
		}
	}

	if index == len(s.operations) {
		s.operations = append(s.operations, s.do(op))
		return nil
	}

	s.operations = append(s.operations[:index+1], s.operations[index:]...)
	for i := len(s.operations) - 1; i > index; i-- {
		s.undo(&s.operations[i])
	}

	s.operations[index] = s.do(op)

	for i := index + 1; i < len(s.operations); i++ {
		s.operations[i] = s.do(&s.operations[i].Move)
	}
	return nil
}

// do performs a single move operation on a tree.
func (s *state) do(op *Move) move {
	lm := move{
		Move: Move{
			Parent: op.Parent,
			Meta:   op.Meta,
			Child:  op.Child,
		},
	}

	shouldPut := !s.tree.isAncestor(op.Child, op.Parent)
	p, ok := s.tree.infoMap[op.Child]
	if ok {
		lm.HasOld = true
		lm.Old = p
	}

	if !shouldPut {
		return lm
	}

	if !ok {
		p.Meta.Time = op.Time
	} else {
		s.removeChild(op.Child, p.Parent)
	}

	p.Meta = op.Meta
	p.Parent = op.Parent
	s.tree.infoMap[op.Child] = p
	s.tree.childMap[op.Parent] = append(s.tree.childMap[op.Parent], op.Child)

	return lm
}

func (s *state) removeChild(child, parent Node) {
	oldChildren := s.tree.childMap[parent]
	for i := range oldChildren {
		if oldChildren[i] == child {
			s.tree.childMap[parent] = append(oldChildren[:i], oldChildren[i+1:]...)
			break
		}
	}
}

func (s *state) timestamp(pos, size int) Timestamp {
	if len(s.operations) == 0 {
		return nextTimestamp(0, uint64(pos), uint64(size))
	}
	return nextTimestamp(s.operations[len(s.operations)-1].Time, uint64(pos), uint64(size))
}

func (s *state) findSpareID() Node {
	id := uint64(1)
	for _, ok := s.infoMap[id]; ok; _, ok = s.infoMap[id] {
		id++
	}
	return id
}

// tree is a mapping from the child nodes to their parent and metadata.
type tree struct {
	infoMap  map[Node]nodeInfo
	childMap map[Node][]Node
}

func newTree() *tree {
	return &tree{
		childMap: make(map[Node][]Node),
		infoMap:  make(map[Node]nodeInfo),
	}
}

// isAncestor returns true if parent is an ancestor of a child.
// For convenience, also return true if parent == child.
func (t tree) isAncestor(parent, child Node) bool {
	for c := child; c != parent; {
		p, ok := t.infoMap[c]
		if !ok {
			return false
		}
		c = p.Parent
	}
	return true
}

// getPathPrefix descends by path constructed from values of attr until
// there is no node corresponding to a path element. Returns the amount of nodes
// processed and ID of the last node.
func (t tree) getPathPrefix(attr string, path []string) (int, Node) {
	var curNode Node

loop:
	for i := range path {
		children := t.childMap[curNode]
		for j := range children {
			meta := t.infoMap[children[j]].Meta
			f := meta.GetAttr(attr)
			if len(meta.Items) == 1 && string(f) == path[i] {
				curNode = children[j]
				continue loop
			}
		}
		return i, curNode
	}

	return len(path), curNode
}

// get returns list of nodes which have the specified path from root
// descending by values of attr from meta.
func (t tree) get(attr string, path []string, latest bool) []Node {
	if len(path) == 0 {
		return nil
	}

	i, curNode := t.getPathPrefix(attr, path[:len(path)-1])
	if i < len(path)-1 {
		return nil
	}

	var nodes []Node
	var lastTs Timestamp

	children := t.childMap[curNode]
	for i := range children {
		info := t.infoMap[children[i]]
		fileName := string(info.Meta.GetAttr(attr))
		if fileName == path[len(path)-1] {
			if latest {
				if info.Meta.Time >= lastTs {
					nodes = append(nodes[:0], children[i])
				}
			} else {
				nodes = append(nodes, children[i])
			}
		}
	}

	return nodes
}

// getMeta returns meta information of node n.
func (t tree) getMeta(n Node) Meta {
	return t.infoMap[n].Meta
}
