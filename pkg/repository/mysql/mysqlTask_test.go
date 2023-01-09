package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLTaskIDScan(t *testing.T) {
	t.Parallel()

	var mySQLID TaskID

	err := mySQLID.Scan(nil)
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{})
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{12})
	assert.NotNil(t, err)

	err = mySQLID.Scan("test")
	assert.NotNil(t, err)
}
