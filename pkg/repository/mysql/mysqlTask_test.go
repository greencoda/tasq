package mysql_test

import (
	"testing"

	"github.com/greencoda/tasq/pkg/repository/mysql"
	"github.com/stretchr/testify/assert"
)

func TestMySQLTaskIDScan(t *testing.T) {
	t.Parallel()

	var mySQLID mysql.TaskID

	err := mySQLID.Scan(nil)
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{})
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{12})
	assert.NotNil(t, err)

	err = mySQLID.Scan("test")
	assert.NotNil(t, err)
}
