package object

func (s *objectService) RelativeAvailableCap() float64 {
	diff := float64(s.ls.Size()) / float64(s.storageCap)
	if 1-diff < 0 {
		return 0
	}

	return 1 - diff
}

func (s *objectService) AbsoluteAvailableCap() uint64 {
	localSize := uint64(s.ls.Size())
	if localSize > s.storageCap {
		return 0
	}

	return s.storageCap - localSize
}
