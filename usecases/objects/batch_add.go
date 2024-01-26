//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"fmt"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	cloud_utils "github.com/weaviate/weaviate/cloud/utils"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

var errEmptyObjects = NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least one object for batching")

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	ctx = classcache.ContextWithClassCache(ctx)

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	before := time.Now()
	b.metrics.BatchInc()
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	return b.addObjects(ctx, principal, objects, fields, repl)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	beforePreProcessing := time.Now()
	if err := b.validateObjectForm(objects); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'objects': %v", err)
	}

	batchObjects := b.validateObjectsConcurrently(ctx, principal, objects, fields, repl)

	if err := b.autoSchemaManager.autoTenants(ctx, principal, objects); err != nil {
		return nil, fmt.Errorf("auto create tenants: %w", err)
	}

	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var res BatchObjects

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())

	// Ensure that the local schema has caught up to the version we used to validate
	if err := b.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects, repl, maxSchemaVersion); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateAndGetVector(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties,
) (BatchObjects, uint64) {
	var (
		now          = time.Now().UnixNano() / int64(time.Millisecond)
		batchObjects = make(BatchObjects, len(objects))

		objectsPerClass       = make(map[string][]*models.Object)
		classPerClassName     = make(map[string]*models.Class)
		originalIndexPerClass = make(map[string][]int)
		validator             = validation.New(b.vectorRepo.Exists, b.config, repl)
	)

	// validate each object and sort by class (==vectorizer)
	var maxSchemaVersion uint64
	for i, obj := range objects {
		batchObjects[i].OriginalIndex = i

		schemaVersion, err := b.autoSchemaManager.autoSchema(ctx, principal, true, obj)
		if err != nil {
			batchObjects[i].Err = err
		}
		if schemaVersion > maxSchemaVersion {
			maxSchemaVersion = schemaVersion
		}

		if obj.ID == "" {
			// Generate UUID for the new object
			uid, err := generateUUID()
			obj.ID = uid
			batchObjects[i].Err = err
		} else {
			if _, err := uuid.Parse(obj.ID.String()); err != nil {
				batchObjects[i].Err = err
			}
		}
		if obj.Properties == nil {
			obj.Properties = map[string]interface{}{}
		}
		obj.CreationTimeUnix = now
		obj.LastUpdateTimeUnix = now
		batchObjects[i].Object = obj
		batchObjects[i].UUID = obj.ID
		if batchObjects[i].Err != nil {
			continue
		}

		vclasses, err := b.schemaManager.GetCachedClass(ctx, principal, obj.Class)
		if err != nil {
			batchObjects[i].Err = err
			continue
		}
		if len(vclasses) == 0 || vclasses[obj.Class].Class == nil {
			batchObjects[i].Err = fmt.Errorf("class '%v' not present in schema", obj.Class)
			continue
		}
		class := vclasses[obj.Class].Class
		// Set most up-to-date class's schema (in case new properties were added by autoschema)
		// If it was not changed, same class will be fetched from cache
		classPerClassName[obj.Class] = class

		if err := validator.Object(ctx, class, obj, nil); err != nil {
			batchObjects[i].Err = err
			continue
		}

		if objectsPerClass[obj.Class] == nil {
			objectsPerClass[obj.Class] = make([]*models.Object, 0)
			originalIndexPerClass[obj.Class] = make([]int, 0)
		}
		objectsPerClass[obj.Class] = append(objectsPerClass[obj.Class], obj)
		originalIndexPerClass[obj.Class] = append(originalIndexPerClass[obj.Class], i)
	}

	object := &models.Object{}
	object.LastUpdateTimeUnix = 0
	object.ID = id
	object.Vector = concept.Vector
	object.Vectors = concept.Vectors
	object.Tenant = concept.Tenant

	if _, ok := fieldsToKeep["class"]; ok {
		object.Class = concept.Class
	}
	if _, ok := fieldsToKeep["properties"]; ok {
		object.Properties = concept.Properties
	}

	if object.Properties == nil {
		object.Properties = map[string]interface{}{}
	}
	now := unixNow()
	if _, ok := fieldsToKeep["creationTimeUnix"]; ok {
		object.CreationTimeUnix = now
	}
	if _, ok := fieldsToKeep["lastUpdateTimeUnix"]; ok {
		object.LastUpdateTimeUnix = now
	}

	// Batch together the GetClass and the validation function as the validation function will create/update the class
	// if it already exists to match the object.
	err = backoff.Retry(func() error {
		class, err := b.schemaManager.GetClass(ctx, principal, object.Class)
		if err != nil {
			return err
		}

		if class == nil {
			return fmt.Errorf("class '%s' not present in schema", object.Class)
		}

		err = validation.New(b.vectorRepo.Exists, b.config, repl).Object(ctx, class, object, nil)
		if err != nil {
			return err
		}
		// update vector only if we passed validation
		err = b.modulesProvider.UpdateVector(ctx, object, class, b.findObject, b.logger)
		if err != nil {
			return err
		}

		return nil
	}, cloud_utils.NewBackoff())
	ec.Add(err)

	return batchObjects, maxSchemaVersion
}
