package application

import "encoding/json"

type messageMapper struct{}

func newMessageMapper() *messageMapper {
	return &messageMapper{}
}

func (m *messageMapper) marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
