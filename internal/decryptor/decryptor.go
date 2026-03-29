/*
Copyright 2020 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package decryptor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	gcpkmsapi "cloud.google.com/go/kms/apiv1"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fluxcd/pkg/auth"
	"github.com/fluxcd/pkg/auth/aws"
	"github.com/fluxcd/pkg/auth/azure"
	"github.com/fluxcd/pkg/auth/gcp"
	"github.com/fluxcd/pkg/cache"
	"github.com/getsops/sops/v3"
	"github.com/getsops/sops/v3/aes"
	"github.com/getsops/sops/v3/age"
	"github.com/getsops/sops/v3/cmd/sops/common"
	"github.com/getsops/sops/v3/cmd/sops/formats"
	"github.com/getsops/sops/v3/config"
	"github.com/getsops/sops/v3/keyservice"
	"github.com/getsops/sops/v3/pgp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cuev1 "github.com/addreas/cue-controller/api/v1"
	intawskms "github.com/addreas/cue-controller/internal/sops/awskms"
	intazkv "github.com/addreas/cue-controller/internal/sops/azkv"
	intkeyservice "github.com/addreas/cue-controller/internal/sops/keyservice"
)

const (
	// DecryptionProviderSOPS is the SOPS provider name.
	DecryptionProviderSOPS = "sops"
	// DecryptionPGPExt is the extension of the file containing an armored PGP
	// key.
	DecryptionPGPExt = ".asc"
	// DecryptionAgeExt is the extension of the file containing an age key
	// file.
	DecryptionAgeExt = ".agekey"
	// DecryptionVaultTokenFileName is the name of the file containing the
	// Hashicorp Vault token.
	DecryptionVaultTokenFileName = "sops.vault-token"
	// DecryptionAWSKmsFile is the name of the file containing the AWS KMS
	// credentials.
	DecryptionAWSKmsFile = "sops.aws-kms"
	// DecryptionAzureAuthFile is the name of the file containing the Azure
	// credentials.
	DecryptionAzureAuthFile = "sops.azure-kv"
	// DecryptionGCPCredsFile is the name of the file containing the GCP
	// credentials.
	DecryptionGCPCredsFile = "sops.gcp-kms"
	// maxEncryptedFileSize is the max allowed file size in bytes of an encrypted
	// file.
	maxEncryptedFileSize int64 = 5 << 20
	// unsupportedFormat is used to signal no sopsFormatToMarkerBytes format was
	// detected by detectFormatFromMarkerBytes.
	unsupportedFormat = formats.Format(-1)
)

var (
	// sopsFormatToString is the counterpart to
	// https://github.com/mozilla/sops/blob/v3.7.2/cmd/sops/formats/formats.go#L16
	sopsFormatToString = map[formats.Format]string{
		formats.Binary: "binary",
		formats.Dotenv: "dotenv",
		formats.Ini:    "INI",
		formats.Json:   "JSON",
		formats.Yaml:   "YAML",
	}
	// sopsFormatToMarkerBytes contains a list of formats and their byte
	// order markers, used to detect if a Secret data field is SOPS' encrypted.
	sopsFormatToMarkerBytes = map[formats.Format][]byte{
		// formats.Binary is a JSON envelop at encrypted rest
		formats.Binary: []byte("\"mac\": \"ENC["),
		formats.Dotenv: []byte("sops_mac=ENC["),
		formats.Ini:    []byte("[sops]"),
		formats.Json:   []byte("\"mac\": \"ENC["),
		formats.Yaml:   []byte("mac: ENC["),
	}
)

// Decryptor performs decryption operations for a v1.Kustomization.
// The only supported decryption provider at present is
// DecryptionProviderSOPS.
type Decryptor struct {
	// root is the root for file system operations. Any (relative) path or
	// symlink is not allowed to traverse outside this path.
	root string
	// client is the Kubernetes client used to e.g. retrieve Secrets with.
	client client.Client
	// cueExport is the v1.Kustomization we are decrypting for.
	// The v1.Decryption of the object is used to ImportKeys().
	cueExport *cuev1.CueExport
	// maxFileSize is the max size in bytes a file is allowed to have to be
	// decrypted. Defaults to maxEncryptedFileSize.
	maxFileSize int64
	// checkSopsMac instructs the decryptor to perform the SOPS data integrity
	// check using the MAC. Not enabled by default, as arbitrary data gets
	// injected into most resources, causing the integrity check to fail.
	// Mostly kept around for feature completeness and documentation purposes.
	checkSopsMac bool
	// tokenCache is the cache for token credentials.
	tokenCache *cache.TokenCache

	// gnuPGHome is the absolute path of the GnuPG home directory used to
	// decrypt PGP data. When empty, the systems' GnuPG keyring is used.
	// When set, ImportKeys() imports found PGP keys into this keyring.
	gnuPGHome pgp.GnuPGHome
	// ageIdentities is the set of age identities available to the decryptor.
	ageIdentities age.ParsedIdentities
	// vaultToken is the Hashicorp Vault token used to authenticate towards
	// any Vault server.
	vaultToken string
	// awsCredentialsProvider is the AWS credentials provider object used to authenticate
	// towards any AWS KMS.
	awsCredentialsProvider func(region string) awssdk.CredentialsProvider
	// azureTokenCredential is the Azure credential token used to authenticate towards
	// any Azure Key Vault.
	azureTokenCredential azcore.TokenCredential
	// gcpTokenSource is the GCP token source used to authenticate towards
	// any GCP KMS.
	gcpTokenSource oauth2.TokenSource

	// keyServices are the SOPS keyservice.KeyServiceClient's available to the
	// decryptor.
	keyServices      []keyservice.KeyServiceClient
	localServiceOnce sync.Once

	// sopsAgeSecret is the NamespacedName of the Secret containing
	// a fallback SOPS age decryption key.
	sopsAgeSecret *types.NamespacedName
}

// New creates a new Decryptor, with a temporary GnuPG
// home directory to Decryptor.ImportKeys() into.
func New(client client.Client, cueExport *cuev1.CueExport, opts ...Option) (*Decryptor, func(), error) {
	gnuPGHome, err := pgp.NewGnuPGHome()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create decryptor: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(gnuPGHome.String()) }
	d := &Decryptor{
		client:      client,
		cueExport:   cueExport,
		maxFileSize: maxEncryptedFileSize,
		gnuPGHome:   gnuPGHome,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d, cleanup, nil
}

// IsDecryptionDisabled checks if the given object has the decrypt: disabled annotation set
func IsDecryptionDisabled(annotations map[string]string) bool {
	return annotations != nil &&
		strings.EqualFold(annotations[fmt.Sprintf("%s/decrypt", cuev1.GroupVersion.Group)], cuev1.DisabledValue)
}

// IsEncryptedSecret checks if the given object is a Kubernetes Secret encrypted
// with Mozilla SOPS.
func IsEncryptedSecret(object *unstructured.Unstructured) bool {
	if object.GetKind() == "Secret" && object.GetAPIVersion() == "v1" {
		if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "sops"); found {
			return true
		}
	}
	return false
}

// ImportKeys imports the DecryptionProviderSOPS keys from the data values of
// the Secret referenced in the Kustomization's v1.Decryption spec.
// It returns an error if the Secret cannot be retrieved, or if one of the
// imports fails.
// Imports do not have an effect after the first call to SopsDecryptWithFormat(),
// which initializes and caches SOPS' (local) key service server.
// For the import of PGP keys, the Decryptor must be configured with
// an absolute GnuPG home directory path.
func (d *Decryptor) ImportKeys(ctx context.Context) error {
	if d.cueExport.Spec.Decryption == nil ||
		(d.cueExport.Spec.Decryption.SecretRef == nil && d.sopsAgeSecret == nil) {
		return nil
	}

	provider := d.cueExport.Spec.Decryption.Provider
	switch provider {
	case DecryptionProviderSOPS:
		secretRef := d.cueExport.Spec.Decryption.SecretRef

		// We handle the SOPS age global decryption separately, as most of the other
		// decryption providers already support global decryption in other ways, and
		// we don't want to introduce duplicate methods of achieving the same.
		// Furthermore, allowing e.g. cloud provider credentials to be fetched
		// from this global secret would prevent workload identity from working.
		if secretRef == nil && d.sopsAgeSecret != nil {
			var secret corev1.Secret
			if err := d.client.Get(ctx, *d.sopsAgeSecret, &secret); err != nil {
				if apierrors.IsNotFound(err) {
					return err
				}
				return fmt.Errorf("cannot get %s SOPS age decryption Secret '%s': %w", provider, *d.sopsAgeSecret, err)
			}
			for name, value := range secret.Data {
				if filepath.Ext(name) == DecryptionAgeExt {
					if err := d.ageIdentities.Import(string(value)); err != nil {
						return fmt.Errorf("failed to import '%s' data from %s SOPS age decryption Secret '%s': %w",
							name, provider, *d.sopsAgeSecret, err)
					}
				}
			}
			return nil
		}

		secretName := types.NamespacedName{
			Namespace: d.cueExport.GetNamespace(),
			Name:      secretRef.Name,
		}

		var secret corev1.Secret
		if err := d.client.Get(ctx, secretName, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return err
			}
			return fmt.Errorf("cannot get %s decryption Secret '%s': %w", provider, secretName, err)
		}

		var err error
		for name, value := range secret.Data {
			switch filepath.Ext(name) {
			case DecryptionPGPExt:
				if err = d.gnuPGHome.Import(value); err != nil {
					return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
				}
			case DecryptionAgeExt:
				if err = d.ageIdentities.Import(string(value)); err != nil {
					return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
				}
			case filepath.Ext(DecryptionVaultTokenFileName):
				if name == DecryptionVaultTokenFileName {
					token := string(value)
					token = strings.Trim(strings.TrimSpace(token), "\n")
					d.vaultToken = token
				}
			case filepath.Ext(DecryptionAWSKmsFile):
				if name == DecryptionAWSKmsFile {
					awsCreds, err := intawskms.LoadStaticCredentialsFromYAML(value)
					if err != nil {
						return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
					}
					d.awsCredentialsProvider = func(string) awssdk.CredentialsProvider { return awsCreds }
				}
			case filepath.Ext(DecryptionAzureAuthFile):
				if name == DecryptionAzureAuthFile {
					conf := intazkv.AADConfig{}
					if err = intazkv.LoadAADConfigFromBytes(value, &conf); err != nil {
						return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
					}
					azureToken, err := intazkv.TokenCredentialFromAADConfig(conf)
					if err != nil {
						return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
					}
					d.azureTokenCredential = azureToken
				}
			case filepath.Ext(DecryptionGCPCredsFile):
				if name == DecryptionGCPCredsFile {
					creds, err := google.CredentialsFromJSON(ctx,
						bytes.Trim(value, "\n"), gcpkmsapi.DefaultAuthScopes()...)
					if err != nil {
						return fmt.Errorf("failed to import '%s' data from %s decryption Secret '%s': %w", name, provider, secretName, err)
					}
					d.gcpTokenSource = creds.TokenSource
				}
			}
		}
	}
	return nil
}

// SetAuthOptions sets the authentication options for secret-less authentication
// with cloud providers.
func (d *Decryptor) SetAuthOptions(ctx context.Context) {
	if d.cueExport.Spec.Decryption == nil {
		return
	}

	switch d.cueExport.Spec.Decryption.Provider {
	case DecryptionProviderSOPS:
		opts := []auth.Option{
			auth.WithClient(d.client),
		}

		saName := d.cueExport.Spec.Decryption.ServiceAccountName
		if saName == "" {
			saName = auth.GetDefaultDecryptionServiceAccount()
		}
		if saName != "" {
			opts = append(opts, auth.WithServiceAccountName(saName))
			opts = append(opts, auth.WithServiceAccountNamespace(d.cueExport.GetNamespace()))
		}

		involvedObject := cache.InvolvedObject{
			Kind:      cuev1.CueExportKind,
			Name:      d.cueExport.GetName(),
			Namespace: d.cueExport.GetNamespace(),
		}

		if d.awsCredentialsProvider == nil {
			awsOpts := slices.Clone(opts)
			if d.tokenCache != nil {
				involvedObject.Operation = cuev1.MetricDecryptWithAWS
				awsOpts = append(awsOpts, auth.WithCache(*d.tokenCache, involvedObject))
			}
			d.awsCredentialsProvider = func(region string) awssdk.CredentialsProvider {
				awsOptsWithRegion := slices.Clone(awsOpts)
				awsOptsWithRegion = append(awsOptsWithRegion, auth.WithSTSRegion(region))
				return aws.NewCredentialsProvider(ctx, awsOptsWithRegion...)
			}
		}

		if d.azureTokenCredential == nil {
			azureOpts := slices.Clone(opts)
			if d.tokenCache != nil {
				involvedObject.Operation = cuev1.MetricDecryptWithAzure
				azureOpts = append(azureOpts, auth.WithCache(*d.tokenCache, involvedObject))
			}
			d.azureTokenCredential = azure.NewTokenCredential(ctx, azureOpts...)
		}

		if d.gcpTokenSource == nil {
			gcpOpts := slices.Clone(opts)
			if d.tokenCache != nil {
				involvedObject.Operation = cuev1.MetricDecryptWithGCP
				gcpOpts = append(gcpOpts, auth.WithCache(*d.tokenCache, involvedObject))
			}
			d.gcpTokenSource = gcp.NewTokenSource(ctx, gcpOpts...)
		}
	}
}

// SopsDecryptWithFormat attempts to load a SOPS encrypted file using the store
// for the input format, gathers the data key for it from the key service,
// and then decrypts the file data with the retrieved data key.
// It returns the decrypted bytes in the provided output format, or an error.
func (d *Decryptor) SopsDecryptWithFormat(data []byte, inputFormat, outputFormat formats.Format) (_ []byte, err error) {
	defer func() {
		// It was discovered that malicious input and/or output instructions can
		// make SOPS panic. Recover from this panic and return as an error.
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to emit encrypted %s file as decrypted %s: %v",
				sopsFormatToString[inputFormat], sopsFormatToString[outputFormat], r)
		}
	}()

	store := common.StoreForFormat(inputFormat, config.NewStoresConfig())

	tree, err := store.LoadEncryptedFile(data)
	if err != nil {
		return nil, sopsUserErr(fmt.Sprintf("failed to load encrypted %s data", sopsFormatToString[inputFormat]), err)
	}

	metadataKey, err := tree.Metadata.GetDataKeyWithKeyServices(d.keyServiceServer(), sops.DefaultDecryptionOrder)
	if err != nil {
		return nil, sopsUserErr("cannot get sops data key", err)
	}

	cipher := aes.NewCipher()
	mac, err := safeDecrypt(tree.Decrypt(metadataKey, cipher))
	if err != nil {
		return nil, sopsUserErr("error decrypting sops tree", err)
	}

	if d.checkSopsMac {
		// Compute the hash of the cleartext tree and compare it with
		// the one that was stored in the document. If they match,
		// integrity was preserved
		// Ref: github.com/getsops/sops/v3/decrypt/decrypt.go
		originalMac, err := safeDecrypt(cipher.Decrypt(
			tree.Metadata.MessageAuthenticationCode,
			metadataKey,
			tree.Metadata.LastModified.Format(time.RFC3339),
		))
		if err != nil {
			return nil, sopsUserErr("failed to verify sops data integrity", err)
		}
		if originalMac != mac {
			// If the file has an empty MAC, display "no MAC"
			if originalMac == "" {
				originalMac = "no MAC"
			}
			return nil, fmt.Errorf("failed to verify sops data integrity: expected mac '%s', got '%s'", originalMac, mac)
		}
	}

	outputStore := common.StoreForFormat(outputFormat, config.NewStoresConfig())
	out, err := outputStore.EmitPlainFile(tree.Branches)
	if err != nil {
		return nil, sopsUserErr(fmt.Sprintf("failed to emit encrypted %s file as decrypted %s",
			sopsFormatToString[inputFormat], sopsFormatToString[outputFormat]), err)
	}
	return out, err
}

// DecryptSources attempts to decrypt all types.SecretArgs FileSources and
// EnvSources a Kustomization file in the directory at the provided path refers
// to, before walking recursively over all other resources it refers to.
// It ignores resource references which refer to absolute or relative paths
// outside the working directory of the decryptor, but returns any decryption
// error.
func (d *Decryptor) DecryptSources(path string) error {
	if d.cueExport.Spec.Decryption == nil || d.cueExport.Spec.Decryption.Provider != DecryptionProviderSOPS {
		return nil
	}

	return filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
		format := formatForPath(path)
		if !entry.IsDir() && entry.Type().IsRegular() {
			err = d.sopsDecryptFile(path, format, format)
			if err != nil {
				return fmt.Errorf("failed to decrypt %s: %w", path, err)
			}
		}
		return nil
	})
}

// sopsDecryptFile attempts to decrypt the file at the given path using SOPS'
// store for the provided input format, and writes it back to the path using
// the store for the output format.
// Path must be absolute and a regular file, the file is not allowed to exceed
// the maxFileSize.
//
// NB: The method only does the simple checks described above and does not
// verify whether the path provided is inside the working directory. Boundary
// enforcement is expected to have been done by the caller.
func (d *Decryptor) sopsDecryptFile(path string, inputFormat, outputFormat formats.Format) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}

	if fileSize := fi.Size(); d.maxFileSize > 0 && fileSize > d.maxFileSize {
		return fmt.Errorf("cannot decrypt file with size (%d bytes) exceeding limit (%d)", fileSize, d.maxFileSize)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if !bytes.Contains(data, sopsFormatToMarkerBytes[inputFormat]) {
		return nil
	}

	out, err := d.SopsDecryptWithFormat(data, inputFormat, outputFormat)
	if err != nil {
		return err
	}
	err = os.WriteFile(path, out, 0o600)
	if err != nil {
		return fmt.Errorf("error writing sops decrypted %s data to %s file: %w",
			sopsFormatToString[inputFormat], sopsFormatToString[outputFormat], err)
	}
	return nil
}

// keyServiceServer returns the SOPS (local) key service clients used to serve
// decryption requests. loadKeyServiceServer() is only configured on the first
// call.
func (d *Decryptor) keyServiceServer() []keyservice.KeyServiceClient {
	d.localServiceOnce.Do(func() {
		d.loadKeyServiceServer()
	})
	return d.keyServices
}

// loadKeyServiceServer loads the SOPS (local) key service clients used to
// serve decryption requests for the current set of Decryptor
// credentials.
func (d *Decryptor) loadKeyServiceServer() {
	serverOpts := []intkeyservice.ServerOption{
		intkeyservice.WithGnuPGHome(d.gnuPGHome),
		intkeyservice.WithVaultToken(d.vaultToken),
		intkeyservice.WithAgeIdentities(d.ageIdentities),
		intkeyservice.WithAWSCredentialsProvider{CredentialsProvider: d.awsCredentialsProvider},
		intkeyservice.WithAzureTokenCredential{TokenCredential: d.azureTokenCredential},
		intkeyservice.WithGCPTokenSource{TokenSource: d.gcpTokenSource},
	}
	server := intkeyservice.NewServer(serverOpts...)
	d.keyServices = append(make([]keyservice.KeyServiceClient, 0), keyservice.NewCustomLocalClient(server))
}

func sopsUserErr(msg string, err error) error {
	if userErr, ok := err.(sops.UserError); ok {
		err = errors.New(userErr.UserError())
	}
	return fmt.Errorf("%s: %w", msg, err)
}

func formatForPath(path string) formats.Format {
	switch {
	case strings.HasSuffix(path, corev1.DockerConfigJsonKey):
		return formats.Json
	default:
		return formats.FormatForPath(path)
	}
}

// safeDecrypt redacts secret values in sops error messages.
func safeDecrypt[T any](mac T, err error) (T, error) {
	const (
		prefix = "Input string "
		suffix = " does not match sops' data format"
	)

	if err == nil {
		return mac, nil
	}

	var buf strings.Builder

	e := err.Error()
	prefIdx := strings.Index(e, prefix)
	suffIdx := strings.Index(e, suffix)

	var zero T
	if prefIdx == -1 || suffIdx == -1 {
		return zero, err
	}

	buf.WriteString(e[:prefIdx])
	buf.WriteString(prefix)
	buf.WriteString("<redacted>")
	buf.WriteString(suffix)

	return zero, errors.New(buf.String())
}
