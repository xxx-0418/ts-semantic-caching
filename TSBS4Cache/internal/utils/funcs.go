package utils

import (
	"fmt"
	"time"
)

func IsIn(s string, arr []string) bool {
	for _, x := range arr {
		if s == x {
			return true
		}
	}
	return false
}

func ParseUTCTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

const (
	errInvalidGroupsFmt = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errTotalGroupsZero  = "incorrect interleaved groups configuration: total groups = 0"
)

func ValidateGroups(groupID, totalGroupsNum uint) error {
	if totalGroupsNum == 0 {
		// Need at least one group
		return fmt.Errorf(errTotalGroupsZero)
	}
	if groupID >= totalGroupsNum {
		// Need reasonable groupID
		return fmt.Errorf(errInvalidGroupsFmt, groupID, totalGroupsNum)
	}
	return nil
}
