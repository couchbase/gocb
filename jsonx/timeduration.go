package jsonx

import (
	"encoding/json"
	"time"
)

type MillisecondDuration time.Duration

func (d *MillisecondDuration) UnmarshalJSON(data []byte) error {
	var milliseconds int64
	err := json.Unmarshal(data, &milliseconds)
	if err != nil {
		return err
	}
	*d = MillisecondDuration(time.Duration(milliseconds) * time.Millisecond)
	return nil
}

func (d MillisecondDuration) MarshalJSON() ([]byte, error) {
	var milliseconds = int64(time.Duration(d) / time.Millisecond)
	return json.Marshal(milliseconds)
}
