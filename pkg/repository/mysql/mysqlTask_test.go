package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLTaskIDScan(t *testing.T) {
	var mySqlID MySQLTaskID

	err := mySqlID.Scan(nil)
	assert.Nil(t, err)

	err = mySqlID.Scan([]byte{})
	assert.Nil(t, err)

	err = mySqlID.Scan([]byte{12})
	assert.NotNil(t, err)

	err = mySqlID.Scan("test")
	assert.NotNil(t, err)
}
