package age_test

import (
	"sync"
	"testing"

	"github.com/metacubex/mihomo/component/age"
)

func TestAge(t *testing.T) {
	testCases := []struct {
		name string
		gen  func() (string, string, error)
	}{
		{"X25519", age.GenX25519KeyPair},
		{"MLKEM768X25519", age.GenHybridKeyPair},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			secretKey, publicKey, err := tc.gen()
			if err != nil {
				t.Fatal(err)
			}
			t.Log(secretKey, publicKey)
			publicKeys, err := age.ToPublicKeys(secretKey)
			if err != nil {
				t.Fatal(err)
			}
			if len(publicKeys) != 1 {
				t.Fatal("public keys length is not equal to 1")
			}
			if publicKeys[0] != publicKey {
				t.Fatal("public key is not equal")
			}
			rawData := []byte("hello world")
			encryptData, err := age.EncryptBytes(rawData, publicKey)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(string(encryptData))
			decryptData, err := age.DecryptBytes(encryptData, secretKey)
			if err != nil {
				t.Fatal(err)
			}
			if string(decryptData) != string(rawData) {
				t.Fatal("decrypt data is not equal to raw data")
			}
		})
	}
}

func TestExplicitSecretKeysDoNotUseGlobalKeys(t *testing.T) {
	secretKey, publicKey, err := age.GenX25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := age.EncryptBytes([]byte("profile"), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	age.SetGlobalSecretKeys(secretKey)
	t.Cleanup(func() { age.SetGlobalSecretKeys() })

	if _, err = age.DecryptBytesWithSecretKeys(encrypted); err == nil {
		t.Fatal("explicit decryption unexpectedly used the global key")
	}
	decrypted, err := age.DecryptBytes(encrypted)
	if err != nil || string(decrypted) != "profile" {
		t.Fatalf("global key decryption failed: %q, %v", decrypted, err)
	}
}

func TestGlobalSecretKeysConcurrentAccess(t *testing.T) {
	secretKey, publicKey, err := age.GenX25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := age.EncryptBytes([]byte("profile"), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	age.SetGlobalSecretKeys(secretKey)
	t.Cleanup(func() { age.SetGlobalSecretKeys() })

	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if worker%2 == 0 {
					age.SetGlobalSecretKeys(secretKey)
				} else {
					_, _ = age.DecryptBytes(encrypted)
				}
			}
		}(worker)
	}
	wait.Wait()
}
