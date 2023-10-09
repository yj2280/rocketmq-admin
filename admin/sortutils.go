package admin

type SortOpts struct {
	data []Comparable
}

func InitSortCli() *SortOpts {
	return new(SortOpts)
}

func (s *SortOpts) addData(data Comparable) {
	s.data = append(s.data, data)
}

func (s *SortOpts) GetData() []Comparable {
	return s.data
}

func (s *SortOpts) Len() int {
	return len(s.data)
}

func (s *SortOpts) Less(i, j int) bool {
	compare := s.data[i].CompareTo(s.data[j])
	return compare < 0
}

func (s *SortOpts) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
}
