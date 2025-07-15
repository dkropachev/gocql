package gocql

import (
	"github.com/gocql/gocql/address_translator"
	"testing"
	"time"
)

func TestBatch2_Errors(t *testing.T) {
	cluster := createCluster()
	cluster.Authenticator = PasswordAuthenticator{
		Username: "scylla",
		Password: "MNSGZ9Rbva1Xw4u",
	}

	translator := address_translator.NewDNSBasedAddressTranslator(
		"cqlpl.bd4c078c4d.lab.scylla.cloud",
		10*time.Minute,
		nil,
	)

	hosts, err := translator.GetAllEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	cluster.AddressTranslator = translator
	cluster.Hosts = hosts
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_errors (id int primary key, val inet)`); err != nil {
		t.Fatal(err)
	}

	b := session.Batch(LoggedBatch)
	b = b.Query("SELECT * FROM gocql_test.batch_errors WHERE id=2 AND val=?", nil)
	if err := b.Exec(); err == nil {
		t.Fatal("expected to get error for invalid query in batch")
	}
}
