package utils

import (
	"encoding/base64"
	"encoding/json"
)

func Stringify(data any) (string, error) {

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, nil
}

func Reverse[T any](data string) (*T, error) {
	jsonData, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	var result T
	err = json.Unmarshal(jsonData, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
